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

package rpc

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/nuclio/nuclio/pkg/common"
	"github.com/nuclio/nuclio/pkg/common/status"
	"github.com/nuclio/nuclio/pkg/processor/runtime"
	"github.com/nuclio/nuclio/pkg/processwaiter"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/rs/xid"
)

// TODO: Find a better place (both on file system and configuration)
const (
	socketPathTemplate = "/tmp/nuclio-rpc-%s.sock"
	connectionTimeout  = 2 * time.Minute
)

// AbstractRuntime is a runtime that communicates via unix domain socket
type AbstractRuntime struct {
	runtime.AbstractRuntime
	configuration     *runtime.Configuration
	eventEncoder      EventEncoder
	controlEncoder    EventEncoder
	wrapperProcess    *os.Process
	functionLogger    logger.Logger
	runtime           Runtime
	startChan         chan struct{}
	stopChan          chan struct{}
	cancelHandlerChan chan struct{}
	socketType        SocketType
	processWaiter     *processwaiter.ProcessWaiter

	socketAllocator *SocketAllocator
}

type rpcLogRecord struct {
	DateTime string                 `json:"datetime"`
	Level    string                 `json:"level"`
	Message  string                 `json:"message"`
	With     map[string]interface{} `json:"with"`
}

// NewAbstractRuntime returns a new RPC runtime
func NewAbstractRuntime(logger logger.Logger,
	configuration *runtime.Configuration,
	runtimeInstance Runtime) (*AbstractRuntime, error) {
	var err error

	abstractRuntime, err := runtime.NewAbstractRuntime(logger, configuration)
	if err != nil {
		return nil, errors.Wrap(err, "Can't create AbstractRuntime")
	}
	socketAllocator := NewSocketAllocator(logger.GetChild("socketAllocator")

	newRuntime := &AbstractRuntime{
		AbstractRuntime: *abstractRuntime,
		configuration:   configuration,
		runtime:         runtimeInstance,
		startChan:       make(chan struct{}, 1),
		stopChan:        make(chan struct{}, 1),
		socketType:      UnixSocket,
		socketAllocator: socketAllocator,
	}
	socketAllocator.runtime = newRuntime

	return newRuntime, nil
}

func (r *AbstractRuntime) Start() error {
	if err := r.startWrapper(); err != nil {
		r.SetStatus(status.Error)
		return errors.Wrap(err, "Failed to run wrapper")
	}

	r.SetStatus(status.Ready)
	return nil
}

// ProcessEvent processes an event
func (r *AbstractRuntime) ProcessEvent(event nuclio.Event, functionLogger logger.Logger) (interface{}, error) {
	processingResult, err := r.allocateSocketAndWaitForResult(event, functionLogger)
	if err != nil {
		return nil, err
	}
	// this is a single event processing flow, so we only take the first item from the result
	return nuclio.Response{
		Body:        processingResult.results[0].DecodedBody,
		ContentType: processingResult.results[0].ContentType,
		Headers:     processingResult.results[0].Headers,
		StatusCode:  processingResult.results[0].StatusCode,
	}, processingResult.results[0].err
}

// ProcessBatch processes a batch of events
func (r *AbstractRuntime) ProcessBatch(batch []nuclio.Event, functionLogger logger.Logger) ([]*runtime.ResponseWithErrors, error) {
	processingResults, err := r.allocateSocketAndWaitForResult(batch, functionLogger)
	if err != nil {
		return nil, err
	}
	responsesWithErrors := make([]*runtime.ResponseWithErrors, len(processingResults.results))

	for index, processingResult := range processingResults.results {
		if processingResult.EventId == "" {
			functionLogger.WarnWith("Received response with empty event_id, response won't be returned")
			continue
		}
		responsesWithErrors[index] = &runtime.ResponseWithErrors{
			Response: nuclio.Response{
				Body:        processingResult.DecodedBody,
				ContentType: processingResult.ContentType,
				Headers:     processingResult.Headers,
				StatusCode:  processingResult.StatusCode,
			},
			EventId:      processingResult.EventId,
			ProcessError: processingResult.err,
		}
	}

	return responsesWithErrors, nil
}

// Stop stops the runtime
func (r *AbstractRuntime) Stop() error {
	r.Logger.WarnWith("Stopping",
		"status", r.GetStatus(),
		"wrapperProcess", r.wrapperProcess)

	if r.wrapperProcess != nil {

		// stop waiting for process
		if err := r.processWaiter.Cancel(); err != nil {
			r.Logger.WarnWith("Failed to cancel process waiting")
		}

		r.Logger.WarnWith("Killing wrapper process", "wrapperProcessPid", r.wrapperProcess.Pid)
		if err := r.wrapperProcess.Kill(); err != nil {
			r.SetStatus(status.Error)
			return errors.Wrap(err, "Can't kill wrapper process")
		}
	}

	r.waitForProcessTermination(10 * time.Second)

	r.wrapperProcess = nil

	r.SetStatus(status.Stopped)
	r.Logger.Warn("Successfully stopped wrapper process")
	return nil
}

// Restart restarts the runtime
func (r *AbstractRuntime) Restart() error {
	if err := r.Stop(); err != nil {
		return err
	}

	if err := r.startWrapper(); err != nil {
		r.SetStatus(status.Error)
		return errors.Wrap(err, "Can't start wrapper process")
	}

	r.SetStatus(status.Ready)
	return nil
}

// GetSocketType returns the type of socket the runtime works with (unix/tcp)
func (r *AbstractRuntime) GetSocketType() SocketType {
	return r.socketType
}

// WaitForStart returns whether the runtime supports sending an indication that it started
func (r *AbstractRuntime) WaitForStart() bool {
	return false
}

// SupportsRestart returns true if the runtime supports restart
func (r *AbstractRuntime) SupportsRestart() bool {
	return true
}

// SupportsControlCommunication returns true if the runtime supports control communication
func (r *AbstractRuntime) SupportsControlCommunication() bool {
	return false
}

// Drain signals to the runtime to drain its accumulated events and waits for it to finish
func (r *AbstractRuntime) Drain() error {
	// we use SIGUSR2 to signal the wrapper process to drain events
	if err := r.signal(syscall.SIGUSR2); err != nil {
		return errors.Wrap(err, "Failed to signal wrapper process to drain")
	}

	// wait for process to finish event handling or timeout
	// TODO: replace the following function with one that waits for a control communication message or timeout
	r.waitForProcessTermination(r.configuration.WorkerTerminationTimeout)

	return nil
}

// Continue signals the runtime to continue event processing
func (r *AbstractRuntime) Continue() error {
	// we use SIGCONT to signal the wrapper process to continue event processing
	if err := r.signal(syscall.SIGCONT); err != nil {
		return errors.Wrap(err, "Failed to signal wrapper process to continue")
	}

	return nil
}

// Terminate signals to the runtime process that processor is about to stop working
func (r *AbstractRuntime) Terminate() error {

	// we use SIGUSR1 to signal the wrapper process to terminate
	if err := r.signal(syscall.SIGUSR1); err != nil {
		return errors.Wrap(err, "Failed to signal wrapper process to terminate")
	}

	// wait for process to finish event handling or timeout
	// TODO: replace the following function with one that waits for a control communication message or timeout
	r.waitForProcessTermination(r.configuration.WorkerTerminationTimeout)

	return nil
}

func (r *AbstractRuntime) allocateSocketAndWaitForResult(item interface{}, functionLogger logger.Logger) (*batchedResults, error) {

	if currentStatus := r.GetStatus(); currentStatus != status.Ready {
		return nil, errors.Errorf("Processor not ready (current status: %s)", currentStatus)
	}

	r.functionLogger = functionLogger

	// We don't use defer to reset r.functionLogger since it decreases performance
	if err := r.eventEncoder.Encode(item); err != nil {
		r.functionLogger = nil
		return nil, errors.Wrapf(err, "Can't encode item: %+v", item)
	}

	processingResults, ok := <-r.resultChan
	r.functionLogger = nil
	if !ok {
		msg := "Client disconnected"
		r.Logger.Error(msg)
		r.SetStatus(status.Error)
		r.functionLogger = nil
		return nil, errors.New(msg)
	}
	// if processingResults.err is not nil, it means that whole batch processing was failed
	if processingResults.err != nil {
		return nil, processingResults.err
	}
	return processingResults, nil
}

func (r *AbstractRuntime) signal(signal syscall.Signal) error {

	if r.wrapperProcess != nil {
		r.Logger.DebugWith("Signaling wrapper process",
			"pid", r.wrapperProcess.Pid,
			"signal", signal.String())

		if err := r.wrapperProcess.Signal(signal); err != nil {
			r.Logger.WarnWith("Failed to signal wrapper process",
				"pid", r.wrapperProcess.Pid,
				"signal", signal.String())
		}
	} else {
		r.Logger.DebugWith("No wrapper process exists, skipping signal")
	}

	return nil
}

func (r *AbstractRuntime) startWrapper() error {
	err := r.socketAllocator.start(); if err != nil {
		return errors.Wrap(err, "Failed to start socket allocator")
	}

	r.processWaiter, err = processwaiter.NewProcessWaiter()
	if err != nil {
		return errors.Wrap(err, "Failed to create process waiter")
	}

	wrapperProcess, err := r.runtime.RunWrapper(r.socketAllocator.GetSocketAddresses())
	if err != nil {
		return errors.Wrap(err, "Can't run wrapper")
	}

	r.wrapperProcess = wrapperProcess

	go r.watchWrapperProcess()

	// event connection
	eventConnection.conn, err = eventConnection.listener.Accept()
	if err != nil {
		return errors.Wrap(err, "Can't get connection from wrapper")
	}

	r.Logger.InfoWith("Wrapper connected",
		"wid", r.Context.WorkerID,
		"pid", r.wrapperProcess.Pid)

	r.eventEncoder = r.runtime.GetEventEncoder(eventConnection.conn)
	r.resultChan = make(chan *batchedResults)
	r.cancelHandlerChan = make(chan struct{})
	go r.eventWrapperOutputHandler(eventConnection.conn, r.resultChan)

	// control connection
	if r.runtime.SupportsControlCommunication() {

		r.Logger.DebugWith("Creating control connection",
			"wid", r.Context.WorkerID)
		controlConnection.conn, err = controlConnection.listener.Accept()
		if err != nil {
			return errors.Wrap(err, "Can't get control connection from wrapper")
		}

		r.controlEncoder = r.runtime.GetEventEncoder(controlConnection.conn)

		// initialize control message broker
		r.ControlMessageBroker = NewRpcControlMessageBroker(r.controlEncoder, r.Logger, r.configuration.ControlMessageBroker)

		go r.controlOutputHandler(controlConnection.conn)

		r.Logger.DebugWith("Control connection created",
			"wid", r.Context.WorkerID)
	}

	// wait for start if required to
	if r.runtime.WaitForStart() {
		r.Logger.Debug("Waiting for start")

		<-r.startChan
	}

	r.Logger.Debug("Started")

	return nil
}

// Create a listener on unix domain docker, return listener, path to socket and error
func (r *AbstractRuntime) createSocketConnection(connection *socketConnection) error {
	var err error
	if r.runtime.GetSocketType() == UnixSocket {
		connection.listener, connection.address, err = r.createUnixListener()
	} else {
		connection.listener, connection.address, err = r.createTCPListener()
	}

	if err != nil {
		return errors.Wrap(err, "Can't create listener")
	}

	return nil
}

// Create a listener on unix domain docker, return listener, path to socket and error
func (r *AbstractRuntime) createUnixListener() (net.Listener, string, error) {
	socketPath := fmt.Sprintf(socketPathTemplate, xid.New().String())

	if common.FileExists(socketPath) {
		if err := os.Remove(socketPath); err != nil {
			return nil, "", errors.Wrapf(err, "Can't remove socket at %q", socketPath)
		}
	}

	r.Logger.DebugWith("Creating listener socket", "path", socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, "", errors.Wrapf(err, "Can't listen on %s", socketPath)
	}

	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		return nil, "", fmt.Errorf("Can't get underlying Unix listener")
	}

	if err = unixListener.SetDeadline(time.Now().Add(connectionTimeout)); err != nil {
		return nil, "", errors.Wrap(err, "Can't set deadline")
	}

	return listener, socketPath, nil
}

// Create a listener on TCP docker, return listener, port and error
func (r *AbstractRuntime) createTCPListener() (net.Listener, string, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, "", errors.Wrap(err, "Can't find free port")
	}

	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		return nil, "", errors.Wrap(err, "Can't get underlying TCP listener")
	}
	if err = tcpListener.SetDeadline(time.Now().Add(connectionTimeout)); err != nil {
		return nil, "", errors.Wrap(err, "Can't set deadline")
	}

	port := listener.Addr().(*net.TCPAddr).Port

	return listener, fmt.Sprintf("%d", port), nil
}

func (r *AbstractRuntime) controlOutputHandler(conn io.Reader) {

	// recover from panic in case of error
	defer common.CatchAndLogPanicWithOptions(context.Background(), // nolint: errcheck
		r.Logger,
		"control wrapper output handler (Restart called?)",
		&common.CatchAndLogPanicOptions{
			Args:          nil,
			CustomHandler: nil,
		})
	defer func() {
		r.cancelHandlerChan <- struct{}{}
	}()

	outReader := bufio.NewReader(conn)

	// keep a counter for log throttling
	errLogCounter := 0
	logCounterTime := time.Now()

	for {
		select {

		// TODO: sync between event and control output handlers using a shared context
		case <-r.cancelHandlerChan:
			r.Logger.Warn("Control output handler was canceled (Restart called?)")
			return

		default:

			// read control message
			controlMessage, err := r.ControlMessageBroker.ReadControlMessage(outReader)
			if err != nil {

				// if enough time has passed, log the error
				if time.Since(logCounterTime) > 500*time.Millisecond {
					logCounterTime = time.Now()
					errLogCounter = 0
				}
				if errLogCounter%5 == 0 {
					r.Logger.WarnWith(string(common.FailedReadControlMessage),
						"errRootCause", errors.RootCause(err).Error())
					errLogCounter++
				}

				// if error is EOF it means the connection was closed, so we should exit
				if errors.RootCause(err) == io.EOF {
					r.Logger.Debug("Control connection was closed")
					return
				}

				continue
			} else {
				errLogCounter = 0
			}

			r.Logger.DebugWith("Received control message", "messageKind", controlMessage.Kind)

			// send message to control consumers
			if err := r.GetControlMessageBroker().SendToConsumers(controlMessage); err != nil {
				r.Logger.WarnWith("Failed to send control message to consumers", "err", err.Error())
			}

			// TODO: validate and respond to wrapper process
		}
	}
}

func (r *AbstractRuntime) handleStart() {
	r.startChan <- struct{}{}
}

// resolveFunctionLogger return either functionLogger if provided or root Logger if not
func (r *AbstractRuntime) resolveFunctionLogger() logger.Logger {
	if r.functionLogger == nil {
		return r.Logger
	}
	return r.functionLogger
}

func (r *AbstractRuntime) watchWrapperProcess() {

	// whatever happens, clear wrapper process
	defer func() {
		r.stopChan <- struct{}{}
	}()

	// wait for the process
	processWaitResult := <-r.processWaiter.Wait(r.wrapperProcess, nil)

	// if we were simply canceled, do nothing
	if processWaitResult.Err == processwaiter.ErrCancelled {
		r.Logger.DebugWith("Process watch cancelled. Returning",
			"pid", r.wrapperProcess.Pid,
			"wid", r.Context.WorkerID)
		return
	}

	// if process exited gracefully (i.e. wasn't force killed), do nothing
	if processWaitResult.Err == nil && processWaitResult.ProcessState.Success() {
		r.Logger.DebugWith("Process watch done - process exited successfully")
		return
	}

	r.Logger.ErrorWith(string(common.UnexpectedTerminationChildProcess),
		"error", processWaitResult.Err,
		"status", processWaitResult.ProcessState.String())

	var panicMessage string
	if processWaitResult.Err != nil {
		panicMessage = processWaitResult.Err.Error()
	} else {
		panicMessage = processWaitResult.ProcessState.String()
	}

	panic(fmt.Sprintf("Wrapper process for worker %d exited unexpectedly with: %s", r.Context.WorkerID, panicMessage))
}

// waitForProcessTermination will best effort wait few seconds to stop channel, if timeout - assume closed
func (r *AbstractRuntime) waitForProcessTermination(timeout time.Duration) {
	r.Logger.DebugWith("Waiting for process termination",
		"wid", r.Context.WorkerID,
		"process", r.wrapperProcess,
		"timeout", timeout.String())

	for {
		select {
		case <-r.stopChan:
			r.Logger.DebugWith("Process terminated",
				"wid", r.Context.WorkerID,
				"process", r.wrapperProcess)
			return
		case <-time.After(timeout):
			r.Logger.DebugWith("Timeout waiting for process termination, assuming closed",
				"wid", r.Context.WorkerID,
				"process", r.wrapperProcess)
			return
		}
	}
}

func unmarshalResponseData(logger logger.Logger, data []byte, unmarshalledResults *batchedResults) {
	var results []*result

	// define method to process a single result
	handleSingleUnmarshalledResult := func(unmarshalledResult *result) {
		switch unmarshalledResult.BodyEncoding {
		case "text":
			unmarshalledResult.DecodedBody = []byte(unmarshalledResult.Body)
		case "base64":
			unmarshalledResult.DecodedBody, unmarshalledResults.err = base64.StdEncoding.DecodeString(unmarshalledResult.Body)
		default:
			unmarshalledResult.err = fmt.Errorf("Unknown body encoding - %q", unmarshalledResult.BodyEncoding)
		}
	}

	if unmarshalledResults.err = json.Unmarshal(data, &results); unmarshalledResults.err != nil {
		// try to unmarshall data as a single result
		var singleResult *result
		if unmarshalledResults.err = json.Unmarshal(data, &singleResult); unmarshalledResults.err != nil {
			logger.DebugWith("Failed to unmarshal result",
				"err", unmarshalledResults.err.Error())
			return
		} else {
			handleSingleUnmarshalledResult(singleResult)
			unmarshalledResults.results = append(unmarshalledResults.results, singleResult)
			return
		}
	}

	unmarshalledResults.results = results
	for _, unmarshalledResult := range unmarshalledResults.results {
		handleSingleUnmarshalledResult(unmarshalledResult)
	}
}
