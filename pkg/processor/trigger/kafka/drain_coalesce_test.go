//go:build test_unit

/*
Copyright 2026 The Nuclio Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nuclio/nuclio/pkg/processor/controlcommunication"
	"github.com/nuclio/nuclio/pkg/processor/eventprocessor"
	"github.com/nuclio/nuclio/pkg/processor/statistics"
	"github.com/nuclio/nuclio/pkg/processor/trigger"

	"github.com/nuclio/logger"
	nucliozap "github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
)

// coalesceTestWorker is a minimal EventProcessor whose Subscribe is backed by a real control-message
// broker, so drain acknowledgements are delivered exactly as the RPC reader would in production.
type coalesceTestWorker struct {
	*eventprocessor.MockEventProcessor
	index  int
	broker *controlcommunication.ControlMessageBrokerBase
}

func (w *coalesceTestWorker) GetIndex() int { return w.index }

func (w *coalesceTestWorker) Subscribe(kind controlcommunication.ControlMessageKind) (controlcommunication.Subscription, error) {
	return w.broker.Subscribe(kind)
}

func (w *coalesceTestWorker) acknowledgeDrain() error {
	return w.broker.SendToConsumers(&controlcommunication.ControlMessage{
		Kind:       controlcommunication.DrainMessageKind,
		Attributes: map[string]interface{}{"workerId": strconv.Itoa(w.index)},
	})
}

// coalesceTestAllocator counts how many times SignalDraining runs (i.e. how many underlying Drain
// executions happened) and blocks each execution on gate so the test can hold a drain in flight
// while concurrent callers arrive. Each worker acknowledges exactly once per execution.
type coalesceTestAllocator struct {
	workers     []*coalesceTestWorker
	signalCount atomic.Int64
	gate        chan struct{}
}

func (a *coalesceTestAllocator) SignalDraining() error {
	a.signalCount.Add(1)
	if a.gate != nil {
		<-a.gate
	}
	for _, worker := range a.workers {
		if err := worker.acknowledgeDrain(); err != nil {
			return err
		}
	}
	return nil
}

func (a *coalesceTestAllocator) GetObjects() []eventprocessor.EventProcessor {
	objects := make([]eventprocessor.EventProcessor, 0, len(a.workers))
	for _, worker := range a.workers {
		objects = append(objects, worker)
	}
	return objects
}

func (a *coalesceTestAllocator) Allocate(time.Duration) (eventprocessor.EventProcessor, error) {
	return nil, nil
}
func (a *coalesceTestAllocator) Release(eventprocessor.EventProcessor)            {}
func (a *coalesceTestAllocator) SetObjects([]eventprocessor.EventProcessor) error { return nil }
func (a *coalesceTestAllocator) GetNumObjectsAvailable() int                      { return len(a.workers) }
func (a *coalesceTestAllocator) GetStatistics() *statistics.AllocatorStatistics   { return nil }
func (a *coalesceTestAllocator) SignalContinue() error                            { return nil }
func (a *coalesceTestAllocator) SignalTermination() error                         { return nil }
func (a *coalesceTestAllocator) Stop() error                                      { return nil }
func (a *coalesceTestAllocator) IsTerminated() bool                               { return false }

type DrainCoalesceTestSuite struct {
	suite.Suite
	logger logger.Logger
}

func (suite *DrainCoalesceTestSuite) SetupTest() {
	suite.logger, _ = nucliozap.NewNuclioZapTest("test")
}

func (suite *DrainCoalesceTestSuite) newKafka(allocator *coalesceTestAllocator, numWorkers int) *kafka {
	for i := 0; i < numWorkers; i++ {
		allocator.workers = append(allocator.workers, &coalesceTestWorker{
			MockEventProcessor: &eventprocessor.MockEventProcessor{},
			index:              i,
			broker:             controlcommunication.NewControlMessageBrokerBase(),
		})
	}
	k := &kafka{
		AbstractTrigger: trigger.AbstractTrigger{Logger: suite.logger, WorkerAllocator: allocator},
		ctx:             context.Background(),
	}
	// Setup normally installs the per-session drain guard; do it here so drainWorkersOnce has one
	k.rebalanceDrain.Store(&sessionDrainState{})
	return k
}

// TestConcurrentDrainsCoalesceToSingleExecution is the regression guard for NUC-825: many
// ConsumeClaim goroutines (and Cleanup) all call drainWorkersOnce during one rebalance, and they
// must share a single underlying Drain rather than each subscribing to every worker and receiving
// every acknowledgement. We assert SignalDraining ran exactly once for many concurrent callers, and
// that every caller still got the full drained set.
func (suite *DrainCoalesceTestSuite) TestConcurrentDrainsCoalesceToSingleExecution() {
	const numWorkers = 8
	const numCallers = 20

	allocator := &coalesceTestAllocator{gate: make(chan struct{})}
	k := suite.newKafka(allocator, numWorkers)

	results := make([]map[string]struct{}, numCallers)
	errs := make([]error, numCallers)

	// caller 0 starts the shared drain and blocks inside SignalDraining on the gate
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0], errs[0] = k.drainWorkersOnce(5 * time.Second)
	}()

	// wait until the single underlying drain is in flight (blocked on the gate)
	suite.Require().Eventually(func() bool {
		return allocator.signalCount.Load() == 1
	}, 2*time.Second, 5*time.Millisecond, "underlying drain did not start")

	// the remaining callers arrive while that drain is still in flight; they must coalesce onto it
	for caller := 1; caller < numCallers; caller++ {
		wg.Add(1)
		go func(caller int) {
			defer wg.Done()
			results[caller], errs[caller] = k.drainWorkersOnce(5 * time.Second)
		}(caller)
	}

	// let the late callers reach once.Do (they block there behind the first caller) before releasing
	// the gate that the first caller's drain is blocked on
	time.Sleep(100 * time.Millisecond)
	close(allocator.gate)
	wg.Wait()

	// the whole point: one physical drain for all callers, not one per caller
	suite.Require().Equal(int64(1), allocator.signalCount.Load(),
		"concurrent drains must coalesce into a single underlying Drain")

	// every caller still gets the complete, converged drained set
	for caller := 0; caller < numCallers; caller++ {
		suite.Require().NoError(errs[caller], "caller %d", caller)
		suite.Require().Len(results[caller], numWorkers, "caller %d did not get the full drained set", caller)
	}
}

// TestEachSessionDrainsAgain asserts the guard is per-session: within one session repeated calls
// drain once, but a new session (Setup installs a fresh guard) drains again - so consecutive
// rebalances are each able to drain.
func (suite *DrainCoalesceTestSuite) TestEachSessionDrainsAgain() {
	const numWorkers = 4

	allocator := &coalesceTestAllocator{}
	k := suite.newKafka(allocator, numWorkers)

	for round := 0; round < 3; round++ {
		// a new consumer-group session: Setup installs a fresh guard
		k.rebalanceDrain.Store(&sessionDrainState{})

		// several callers within the session share one drain
		for caller := 0; caller < 3; caller++ {
			drained, err := k.drainWorkersOnce(5 * time.Second)
			suite.Require().NoError(err)
			suite.Require().Len(drained, numWorkers)
		}
	}

	// 3 sessions x (many callers) => exactly 3 underlying drains
	suite.Require().Equal(int64(3), allocator.signalCount.Load(),
		"each session must drain exactly once regardless of how many callers request it")
}

func TestDrainCoalesceTestSuite(t *testing.T) {
	suite.Run(t, new(DrainCoalesceTestSuite))
}
