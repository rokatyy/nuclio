/*
Copyright 2024 The Nuclio Authors.

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
	"fmt"
	"net"
	"os"
	"time"

	"github.com/nuclio/nuclio/pkg/common"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	"github.com/rs/xid"
)

const (
	socketPathTemplate = "/tmp/nuclio-rpc-%s.sock"
	connectionTimeout  = 2 * time.Minute
)

type SocketAllocator struct {
	runtime *AbstractRuntime
	logger  logger.Logger

	minSocketsNum        int
	maxSocketsNum        int
	eventSockets         []*EventSocket
	controlMessageSocket *ControlMessageSocket
}

func NewSocketAllocator(logger logger.Logger) *SocketAllocator {
	return &SocketAllocator{logger: logger, minSocketsNum: 1, maxSocketsNum: 1, eventSockets: make([]*EventSocket, 0)}
}

func (sa *SocketAllocator) startListeners() error {
	var controlConnection *socketConnection
	if sa.runtime.SupportsControlCommunication() {
		if err := sa.createSocketConnection(controlConnection); err != nil {
			return errors.Wrap(err, "Failed to create socket connection")
		}
		sa.controlMessageSocket = NewControlMessageSocket(
			sa.logger.GetChild("ControlMessageSocket"),
			controlConnection,
			sa.runtime)
	}

	for i := 0; i < sa.minSocketsNum; i++ {
		eventConnection := &socketConnection{}
		if err := sa.createSocketConnection(eventConnection); err != nil {
			return errors.Wrap(err, "Failed to create socket connection")
		}
		sa.eventSockets = append(sa.eventSockets, NewEventSocket(sa.logger.GetChild("EventSocket"),
			eventConnection,
			sa.runtime))
	}
	return nil
}

func (sa *SocketAllocator) start() error {
	var err error
	for _, socket := range sa.eventSockets {
		if socket.conn, err = socket.listener.Accept(); err != nil {
			return errors.Wrap(err, "Can't get connection from wrapper")
		}
		socket.encoder = sa.runtime.runtime.GetEventEncoder(socket.conn)
		socket.resultChan = make(chan *batchedResults)
		socket.cancelChan = make(chan struct{})
		go socket.runHandler()
	}
	sa.logger.Debug("Events sockets established connection")

	if sa.runtime.SupportsControlCommunication() {
		sa.logger.DebugWith("Creating control connection",
			"wid", sa.runtime.Context.WorkerID)
		sa.controlMessageSocket.conn, err = sa.controlMessageSocket.listener.Accept()
		if err != nil {
			return errors.Wrap(err, "Can't get control connection from wrapper")
		}
		sa.controlMessageSocket.encoder = sa.runtime.runtime.GetEventEncoder(sa.controlMessageSocket.conn)

		// initialize control message broker
		sa.runtime.ControlMessageBroker = NewRpcControlMessageBroker(
			sa.controlMessageSocket.encoder,
			sa.logger,
			sa.runtime.configuration.ControlMessageBroker)

		go sa.controlMessageSocket.runHandler()

		sa.logger.DebugWith("Control connection created",
			"wid", sa.runtime.Context.WorkerID)
	}

	// wait for start if required to
	if sa.runtime.WaitForStart() {
		sa.logger.Debug("Waiting for start")
		for _, socket := range sa.eventSockets {
			<-socket.startChan
		}
	}

	sa.logger.Debug("Socker allocator started")
	return nil
}

func (sa *SocketAllocator) Allocate() *EventSocket {
	return sa.eventSockets[0]
}

func (sa *SocketAllocator) getSocketAddresses() ([]string, string) {
	eventAddresses := make([]string, 0)

	for _, socket := range sa.eventSockets {
		eventAddresses = append(eventAddresses, socket.address)
	}
	if sa.controlMessageSocket == nil {
		return eventAddresses, ""
	}
	return eventAddresses, sa.controlMessageSocket.address
}

// Create a listener on unix domain docker, return listener, path to socket and error
func (sa *SocketAllocator) createSocketConnection(connection *socketConnection) error {
	var err error
	if sa.runtime.GetSocketType() == UnixSocket {
		connection.listener, connection.address, err = sa.createUnixListener()
	} else {
		connection.listener, connection.address, err = sa.createTCPListener()
	}
	if err != nil {
		return errors.Wrap(err, "Can't create listener")
	}

	return nil
}

// Create a listener on unix domain docker, return listener, path to socket and error
func (sa *SocketAllocator) createUnixListener() (net.Listener, string, error) {
	socketPath := fmt.Sprintf(socketPathTemplate, xid.New().String())

	if common.FileExists(socketPath) {
		if err := os.Remove(socketPath); err != nil {
			return nil, "", errors.Wrapf(err, "Can't remove socket at %q", socketPath)
		}
	}

	sa.logger.DebugWith("Creating listener socket", "path", socketPath)

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
func (sa *SocketAllocator) createTCPListener() (net.Listener, string, error) {
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
