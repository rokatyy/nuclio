//go:build test_integration && test_kube_kafka

/*
Copyright 2023 The Nuclio Authors.

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

// Package kafka holds a standalone k8s integration suite for the kafka trigger. It is isolated
// behind the test_kube_kafka build tag and its own Makefile target so it only runs when the kafka
// trigger or the runtime wrappers change. It spins a single-node KRaft Kafka in-cluster and drives
// the CPU-based autoscaling round-trip that regressed in NUC-825: a kafka-triggered function scales
// up under load and must scale back down to one replica once idle. Before the fix, the rebalances
// caused by scaling churned worker drains (each concurrent per-partition drain subscribed to every
// worker and the broker fanned every acknowledgement out to each subscription), so the function
// never went idle and never scaled back down.
package kafka

import (
	_ "embed"
	"encoding/base64"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/nuclio/nuclio/pkg/common"
	"github.com/nuclio/nuclio/pkg/functionconfig"
	"github.com/nuclio/nuclio/pkg/platform"
	kubesuite "github.com/nuclio/nuclio/pkg/platform/kube/test/suite"
	"github.com/nuclio/nuclio/pkg/processor/util/partitionworker"

	"github.com/nuclio/errors"
	"github.com/rs/xid"
	"github.com/stretchr/testify/suite"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

//go:embed testdata/kafka.yaml
var kafkaManifest string

const (
	kafkaNamespacePlaceholder = "NUCLIO_KAFKA_NAMESPACE"
	kafkaPodName              = "kafka-0"
	kafkaContainerName        = "kafka"
	kafkaBootstrap            = "localhost:9092"

	minReplicas = 1

	// Defaults kept modest so the suite runs on a laptop-sized cluster; the reported repro used many
	// more workers, but the scaling behavior reproduces at any worker count > partitions. Overridable
	// via env for a bigger cluster.
	defaultMaxReplicas = 4
	defaultNumWorkers  = 16

	// Records per production chunk; the background producer emits chunks continuously (throttled)
	// until the function scales up, then stops. Overridable via env.
	defaultLoadRecords = 20000

	// Throttle (records/sec) for the background producer, so the backlog grows slowly enough to drain
	// quickly once production stops, while still keeping the consumer busy.
	loadThroughput = 4000

	// CPU-based HPA target (the "cpu" metric from the reported scenario).
	targetCPUPercent = 50

	scaleUpTimeout   = 6 * time.Minute
	scaleDownTimeout = 12 * time.Minute
)

type KafkaTriggerTestSuite struct {
	kubesuite.KubeTestSuite

	topic         string
	consumerGroup string
	numPartitions int
	numWorkers    int
	maxReplicas   int
	loadRecords   int
}

func (suite *KafkaTriggerTestSuite) SetupSuite() {
	suite.KubeTestSuite.SetupSuite()

	suite.topic = "nuc825-topic"
	suite.consumerGroup = "nuc825-group"
	suite.numWorkers = common.GetEnvOrDefaultInt("NUC825_NUM_WORKERS", defaultNumWorkers)
	suite.maxReplicas = common.GetEnvOrDefaultInt("NUC825_MAX_REPLICAS", defaultMaxReplicas)
	suite.loadRecords = common.GetEnvOrDefaultInt("NUC825_LOAD_RECORDS", defaultLoadRecords)

	// one partition per replica so scaling to maxReplicas is meaningful, and workers > partitions so
	// most workers are idle - the shape that made the rebalance drain fan out
	suite.numPartitions = suite.maxReplicas

	suite.deployKafka()
	suite.createTopic(suite.topic, suite.numPartitions)
}

func (suite *KafkaTriggerTestSuite) TearDownSuite() {
	suite.deleteKafka()
	suite.KubeTestSuite.TearDownSuite()
}

// TestScaleDownAfterLoad reproduces NUC-825: a kafka-triggered function scales up while consuming a
// backlog and must scale back down to minReplicas once the backlog is drained and it is idle. The
// scaling itself causes consumer-group rebalances; before the fix those rebalances churned worker
// drains and restarts, keeping the function permanently busy so the HPA never scaled it down.
//
// Buggy processor: stuck above minReplicas after the load drains -> FAIL (times out).
// Fixed processor: returns to minReplicas -> PASS.
func (suite *KafkaTriggerTestSuite) TestScaleDownAfterLoad() {
	functionName := fmt.Sprintf("nuc825-kafka-%s", xid.New().String())

	suite.DeployFunction(suite.compileKafkaFunctionOptions(functionName), func(*platform.CreateFunctionResult) bool {

		// baseline: the function starts at minReplicas
		suite.waitForReadyPods(functionName, minReplicas, 3*time.Minute)

		// Produce a continuous, throttled stream in the background so CPU stays above target long
		// enough for the HPA to react, and stop as soon as it scales up. Stopping on scale-up keeps
		// the accumulated backlog small (only what piled up until the HPA reacted) so it drains
		// quickly afterwards - a fixed one-shot backlog is either too small to sustain scale-up or too
		// large to drain within the scale-down window.
		stopProducing := make(chan struct{})
		producerDone := make(chan struct{})
		go func() {
			defer close(producerDone)
			for {
				select {
				case <-stopProducing:
					return
				default:
					suite.produceMessagesQuiet(suite.topic, suite.loadRecords)
				}
			}
		}()

		// scale up: the HPA adds replicas while the stream is consumed (this triggers rebalances)
		suite.Logger.InfoWith("Waiting for function to scale up under load", "functionName", functionName)
		suite.WaitForFunctionPods(functionName, scaleUpTimeout, func(pods []v1.Pod) bool {
			ready := countReadyPods(pods)
			suite.Logger.DebugWith("Scale-up progress", "readyPods", ready)
			return ready > minReplicas
		})

		// stop the load and let the small residual backlog drain
		close(stopProducing)
		<-producerDone

		// scale down: once the backlog is drained and the function is idle it must return to
		// minReplicas. This is the regression assertion - on the buggy build the drain churn keeps
		// CPU high and it never comes back down.
		suite.Logger.InfoWith("Load produced; waiting for function to scale back down to minReplicas",
			"functionName", functionName)
		suite.WaitForFunctionPods(functionName, scaleDownTimeout, func(pods []v1.Pod) bool {
			ready := countReadyPods(pods)
			suite.Logger.DebugWith("Scale-down progress", "readyPods", ready)
			return ready == minReplicas
		})

		return true
	})
}

func (suite *KafkaTriggerTestSuite) compileKafkaFunctionOptions(functionName string) *platform.CreateFunctionOptions {
	functionPath := path.Join(suite.GetTestFunctionsDir(),
		"common", "event-recorder", "python", "event_recorder_with_rebalance_explicit_ack.py")
	functionSourceCode, err := os.ReadFile(functionPath)
	suite.Require().NoError(err, "Failed to read function source")

	createFunctionOptions := suite.CompileCreateFunctionOptions(functionName)
	createFunctionOptions.FunctionConfig.Spec.Runtime = "python"
	createFunctionOptions.FunctionConfig.Spec.Handler = "event_recorder_with_rebalance_explicit_ack:handler"
	createFunctionOptions.FunctionConfig.Spec.Build.FunctionSourceCode = base64.StdEncoding.EncodeToString(functionSourceCode)

	// the image is built into the node's local daemon (docker-desktop) or loaded into it (CI minikube),
	// so use it directly rather than pulling from the registry
	createFunctionOptions.FunctionConfig.Spec.ImagePullPolicy = v1.PullIfNotPresent

	// CPU-driven HPA: a small CPU request and a low target so consuming the backlog pushes utilization
	// past the threshold and triggers scale-up; going idle drops it and must trigger scale-down
	createFunctionOptions.FunctionConfig.Spec.MinReplicas = common.Pointer(minReplicas)
	createFunctionOptions.FunctionConfig.Spec.MaxReplicas = common.Pointer(suite.maxReplicas)
	createFunctionOptions.FunctionConfig.Spec.TargetCPU = targetCPUPercent
	createFunctionOptions.FunctionConfig.Spec.Resources = v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("50m"),
			v1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}

	brokerURL := fmt.Sprintf("kafka.%s.svc.cluster.local:9092", suite.Namespace)
	createFunctionOptions.FunctionConfig.Spec.Triggers = map[string]functionconfig.Trigger{
		"my-kafka": {
			Kind:       "kafka-cluster",
			URL:        brokerURL,
			NumWorkers: suite.numWorkers,
			Attributes: map[string]interface{}{
				"topics":               []string{suite.topic},
				"consumerGroup":        suite.consumerGroup,
				"initialOffset":        "earliest",
				"workerAllocationMode": string(partitionworker.AllocationModeStatic),
			},
			ExplicitAckMode:          functionconfig.ExplicitAckModeExplicitOnly,
			WorkerTerminationTimeout: "5s",
		},
	}
	return createFunctionOptions
}

// deployKafka applies the single-node KRaft Kafka manifest into the test namespace and waits for it
// to become ready.
func (suite *KafkaTriggerTestSuite) deployKafka() {
	manifest := strings.ReplaceAll(kafkaManifest, kafkaNamespacePlaceholder, suite.Namespace)

	manifestFile, err := os.CreateTemp("", "nuclio-kafka-*.yaml")
	suite.Require().NoError(err, "Failed to create kafka manifest temp file")
	defer os.Remove(manifestFile.Name()) // nolint: errcheck

	_, err = manifestFile.WriteString(manifest)
	suite.Require().NoError(err, "Failed to write kafka manifest")
	suite.Require().NoError(manifestFile.Close(), "Failed to close kafka manifest")

	suite.Logger.InfoWith("Deploying in-cluster kafka", "namespace", suite.Namespace)
	_, err = suite.ExecuteKubectl([]string{"apply", "-f", manifestFile.Name()}, nil)
	suite.Require().NoError(err, "Failed to apply kafka manifest")

	_, err = suite.ExecuteKubectl([]string{
		"wait", "--for=condition=ready", "pod", kafkaPodName,
		"-n", suite.Namespace, "--timeout=180s",
	}, nil)
	suite.Require().NoError(err, "Kafka broker did not become ready")
}

func (suite *KafkaTriggerTestSuite) deleteKafka() {
	manifest := strings.ReplaceAll(kafkaManifest, kafkaNamespacePlaceholder, suite.Namespace)
	manifestFile, err := os.CreateTemp("", "nuclio-kafka-*.yaml")
	if err != nil {
		suite.Logger.WarnWith("Failed to create kafka manifest temp file for teardown", "err", err)
		return
	}
	defer os.Remove(manifestFile.Name()) // nolint: errcheck

	if _, err := manifestFile.WriteString(manifest); err != nil {
		suite.Logger.WarnWith("Failed to write kafka manifest for teardown", "err", err)
		return
	}
	_ = manifestFile.Close()

	if _, err := suite.ExecuteKubectl([]string{"delete", "-f", manifestFile.Name(), "--ignore-not-found"}, nil); err != nil {
		suite.Logger.WarnWith("Failed to delete kafka resources", "err", err)
	}
}

func (suite *KafkaTriggerTestSuite) createTopic(topic string, partitions int) {
	command := fmt.Sprintf(
		"/opt/kafka/bin/kafka-topics.sh --bootstrap-server %s "+
			"--create --if-not-exists --topic %s --partitions %d --replication-factor 1",
		kafkaBootstrap, topic, partitions)
	_, err := suite.kafkaExec(command)
	suite.Require().NoError(err, "Failed to create kafka topic")
}

// produceMessagesQuiet writes one throttled chunk of records spread across partitions. It is called
// in a loop from a goroutine, so it never fails the test directly (Require is not goroutine-safe);
// errors are logged and the loop stops once the caller closes its stop channel.
func (suite *KafkaTriggerTestSuite) produceMessagesQuiet(topic string, numRecords int) {
	command := fmt.Sprintf(
		"/opt/kafka/bin/kafka-producer-perf-test.sh --topic %s --num-records %d --record-size 256 "+
			"--throughput %d --producer-props bootstrap.servers=%s",
		topic, numRecords, loadThroughput, kafkaBootstrap)
	if _, err := suite.kafkaExec(command); err != nil {
		suite.Logger.WarnWith("Produce chunk failed (continuing)", "err", err)
	}
}

func (suite *KafkaTriggerTestSuite) kafkaExec(command string) (string, error) {
	// the shell runner space-joins all args and runs them through `sh -c`, so the in-pod command must
	// be a single quoted token or it gets word-split and `sh -c` receives only its first word
	result, err := suite.ExecuteKubectl([]string{
		"exec", kafkaPodName, "-c", kafkaContainerName, "-n", suite.Namespace,
		"--", "sh", "-c", "'" + command + "'",
	}, nil)
	if err != nil {
		return "", errors.Wrap(err, "Failed to exec in kafka pod")
	}
	return result.Output, nil
}

// countReadyPods counts function pods that are Running and report a Ready condition, ignoring pods
// that are terminating or not yet ready so scale transitions are measured accurately.
func countReadyPods(pods []v1.Pod) int {
	ready := 0
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != v1.PodRunning {
			continue
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
				ready++
				break
			}
		}
	}
	return ready
}

func (suite *KafkaTriggerTestSuite) waitForReadyPods(functionName string, expected int, duration time.Duration) {
	suite.WaitForFunctionPods(functionName, duration, func(pods []v1.Pod) bool {
		return countReadyPods(pods) == expected
	})
}

func TestKafkaTriggerTestSuite(t *testing.T) {
	if testing.Short() {
		return
	}
	suite.Run(t, new(KafkaTriggerTestSuite))
}
