//go:build test_unit

package connection

import (
	"bytes"
	"context"
	"encoding/base64"
	"github.com/nuclio/logger"
	"github.com/nuclio/nuclio-sdk-go"
	"github.com/nuclio/nuclio/pkg/processor/runtime/rpc/encoder"
	"github.com/nuclio/nuclio/pkg/processor/runtime/rpc/result"
	triggertest "github.com/nuclio/nuclio/pkg/processor/trigger/test"
	nucliozap "github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
	"io"

	"testing"
)

type TestConnectionSuite struct {
	suite.Suite
	logger logger.Logger
	ctx    context.Context
}

func (suite *TestConnectionSuite) SetupTest() {
	var err error
	suite.ctx = context.Background()
	suite.logger, err = nucliozap.NewNuclioZapTest("abstract-connection")
	suite.Require().NoError(err)
}

func (suite *TestConnectionSuite) TestStreamProcessing() {
	mockManager := &MockConnectionManager{}
	mockConfig := ManagerConfigration{
		Kind:                        SocketAllocatorManagerKind,
		SupportControlCommunication: true,
		WaitForStart:                true,
		GetEventEncoderFunc: func(w io.Writer) encoder.EventEncoder {
			return encoder.NewEventJSONEncoder(nil, w)
		},
	}

	mockManager.On("GetConfig").Return(mockConfig).Once()
	connection := NewAbstractEventConnection(suite.logger, mockManager)
	testStreamValue := "test-stream"
	testStreamValueBase64 := base64.StdEncoding.EncodeToString([]byte(testStreamValue))
	responseStream := nuclio.NewResponseStream(testStreamValue, map[string]interface{}{testStreamValue: testStreamValue}, 200)
	go func() {
		connection.resultChan <- result.NewStreamStart(responseStream)
		connection.resultChan <- result.NewBodyOnlyFromBase64([]byte(testStreamValueBase64))
		connection.resultChan <- result.NewBodyOnlyFromBase64([]byte(testStreamValueBase64))
		connection.resultChan <- &result.StreamEnd{}
	}()
	var buffer bytes.Buffer
	connection.encoder = encoder.NewEventJSONEncoder(suite.logger, &buffer)

	processingResult, err := connection.ProcessEvent(&triggertest.TestEvent{}, suite.logger)
	suite.Require().NoError(err)
	suite.Require().Equal(processingResult.GetProcessingResult(), responseStream)

	processingResult, err := connection.ProcessStream(&triggertest.TestEvent{}, suite.logger)
	suite.Require().NoError(err)
	suite.Require().Equal(processingResult.GetProcessingResult(), responseStream)

}

func TestConnection(t *testing.T) {
	suite.Run(t, &TestConnectionSuite{})
}
