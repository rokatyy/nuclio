package rpc

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/nuclio/errors"
	"time"

	"github.com/nuclio/logger"
	"github.com/nuclio/nuclio/pkg/common"
	"io"
	"net"
)

type result struct {
	StatusCode   int                    `json:"status_code"`
	ContentType  string                 `json:"content_type"`
	Body         string                 `json:"body"`
	BodyEncoding string                 `json:"body_encoding"`
	Headers      map[string]interface{} `json:"headers"`
	EventId      string                 `json:"event_id"`

	DecodedBody []byte
	err         error
}

type batchedResults struct {
	results []*result
	err     error
}

func newBatchedResults() *batchedResults {
	return &batchedResults{results: make([]*result, 0)}
}

type socketConnection struct {
	conn     net.Conn
	listener net.Listener
	address  string
}

type ControlMessageSocket struct {
	*AbstractSocket
}

func NewControlMessageSocket(logger logger.Logger, socketConnection *socketConnection, runtime *AbstractRuntime) *ControlMessageSocket {
	abstractSocket := &AbstractSocket{socketConnection: socketConnection, Logger: logger, runtime: runtime}
	return &ControlMessageSocket{AbstractSocket: abstractSocket}
}

func (cm *ControlMessageSocket) runHandler() {

	// recover from panic in case of error
	defer common.CatchAndLogPanicWithOptions(context.Background(), // nolint: errcheck
		cm.Logger,
		"control wrapper output handler (Restart called?)",
		&common.CatchAndLogPanicOptions{
			Args:          nil,
			CustomHandler: nil,
		})
	defer func() {
		cm.cancelChan <- struct{}{}
	}()

	outReader := bufio.NewReader(cm.conn)

	// keep a counter for log throttling
	errLogCounter := 0
	logCounterTime := time.Now()

	for {
		select {

		// TODO: sync between event and control output handlers using a shared context
		case <-cm.cancelChan:
			cm.Logger.Warn("Control output handler was canceled (Restart called?)")
			return

		default:

			// read control message
			controlMessage, err := cm.runtime.ControlMessageBroker.ReadControlMessage(outReader)
			if err != nil {

				// if enough time has passed, log the error
				if time.Since(logCounterTime) > 500*time.Millisecond {
					logCounterTime = time.Now()
					errLogCounter = 0
				}
				if errLogCounter%5 == 0 {
					cm.Logger.WarnWith(string(common.FailedReadControlMessage),
						"errRootCause", errors.RootCause(err).Error())
					errLogCounter++
				}

				// if error is EOF it means the connection was closed, so we should exit
				if errors.RootCause(err) == io.EOF {
					cm.Logger.Debug("Control connection was closed")
					return
				}

				continue
			} else {
				errLogCounter = 0
			}

			cm.Logger.DebugWith("Received control message", "messageKind", controlMessage.Kind)

			// send message to control consumers
			if err := cm.runtime.GetControlMessageBroker().SendToConsumers(controlMessage); err != nil {
				cm.Logger.WarnWith("Failed to send control message to consumers", "err", err.Error())
			}

			// TODO: validate and respond to wrapper process
		}
	}
}

type AbstractSocket struct {
	*socketConnection
	Logger     logger.Logger
	outReader  *bufio.Reader
	runtime    *AbstractRuntime
	encoder    EventEncoder
	cancelChan chan struct{}
}

func NewAbstractSocker(socketConnection *socketConnection, logger logger.Logger, runtime *AbstractRuntime) *AbstractSocket {
	return &AbstractSocket{socketConnection: socketConnection, Logger: logger, runtime: runtime}
}

type EventSocket struct {
	*AbstractSocket
	resultChan chan *batchedResults
	startChan  chan struct{}
}

func NewEventSocket(logger logger.Logger, socketConnection *socketConnection, runtime *AbstractRuntime) *EventSocket {

	abstractSocket := &AbstractSocket{socketConnection: socketConnection, Logger: logger, runtime: runtime}
	return &EventSocket{AbstractSocket: abstractSocket}
}

func (s *EventSocket) waitOutput(conn io.Reader, resultChan chan *batchedResults) {

}

func (s *EventSocket) runHandler() {

	// Reset might close outChan, which will cause panic when sending
	defer common.CatchAndLogPanicWithOptions(context.Background(), // nolint: errcheck
		s.Logger,
		"handling event wrapper output (Restart called?)",
		&common.CatchAndLogPanicOptions{
			Args:          nil,
			CustomHandler: nil,
		})
	defer func() {
		s.cancelChan <- struct{}{}
	}()

	outReader := bufio.NewReader(s.conn)

	// Read logs & output
	for {
		select {

		// TODO: sync between event and control output handlers using a shared context
		case <-s.cancelChan:
			s.Logger.Warn("Event output handler was canceled (Restart called?)")
			return

		default:

			unmarshalledResults := newBatchedResults()
			var data []byte
			data, unmarshalledResults.err = outReader.ReadBytes('\n')

			if unmarshalledResults.err != nil {
				s.Logger.WarnWith(string(common.FailedReadFromEventConnection),
					"err", unmarshalledResults.err.Error())
				s.resultChan <- unmarshalledResults
				continue
			}

			switch data[0] {
			case 'r':
				unmarshalResponseData(s.Logger, data[1:], unmarshalledResults)

				// write back to result channel
				s.resultChan <- unmarshalledResults
			case 'm':
				s.handleResponseMetric(data[1:])
			case 'l':
				s.handleResponseLog(data[1:])
			case 's':
				s.handleStart()
			}
		}
	}
}

func (s *EventSocket) handleResponseMetric(response []byte) {
	var metrics struct {
		DurationSec float64 `json:"duration"`
	}

	loggerInstance := s.runtime.resolveFunctionLogger()
	if err := json.Unmarshal(response, &metrics); err != nil {
		loggerInstance.ErrorWith("Can't decode metric", "error", err)
		return
	}

	if metrics.DurationSec == 0 {
		loggerInstance.ErrorWith("No duration in metrics", "metrics", metrics)
		return
	}

	s.runtime.Statistics.DurationMilliSecondsCount++
	s.runtime.Statistics.DurationMilliSecondsSum += uint64(metrics.DurationSec * 1000)
}

func (s *EventSocket) handleResponseLog(response []byte) {
	var logRecord rpcLogRecord

	if err := json.Unmarshal(response, &logRecord); err != nil {
		s.Logger.ErrorWith("Can't decode log", "error", err)
		return
	}

	loggerInstance := s.runtime.resolveFunctionLogger()
	logFunc := loggerInstance.DebugWith

	switch logRecord.Level {
	case "error", "critical", "fatal":
		logFunc = loggerInstance.ErrorWith
	case "warning":
		logFunc = loggerInstance.WarnWith
	case "info":
		logFunc = loggerInstance.InfoWith
	}

	vars := common.MapToSlice(logRecord.With)
	logFunc(logRecord.Message, vars...)
}

// resolveFunctionLogger return either functionLogger if provided or root Logger if not
func (s *EventSocket) resolveFunctionLogger(functionLogger logger.Logger) logger.Logger {
	if s.runtime.functionLogger == nil {
		return s.runtime.Logger
	}
	return s.runtime.functionLogger
}

func (s *EventSocket) handleStart() {
	s.startChan <- struct{}{}
}
