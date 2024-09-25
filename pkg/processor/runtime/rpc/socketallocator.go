package rpc

import (
	"fmt"
	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	"github.com/nuclio/nuclio/pkg/common"
	"github.com/rs/xid"
	"net"
	"os"
	"time"
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
	return &SocketAllocator{logger: logger}
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

	for i := 0; i <= sa.minSocketsNum; i++ {
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

func (sa *SocketAllocator) GetSocketAddresses() ([]string, string) {
	eventAddresses := make([]string, 0)

	for _, socket := range sa.eventSockets {
		eventAddresses = append(eventAddresses, socket.address)
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
