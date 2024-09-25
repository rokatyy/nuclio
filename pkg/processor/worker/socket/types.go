package socket

import (
	"github.com/nuclio/errors"
	"github.com/nuclio/nuclio-sdk-go"
)

type ResponseWithErrors struct {
	nuclio.Response
	EventId         string
	SubmitError     error
	ProcessError    error
	NoResponseError error
}

var ErrNoResponseFromBatchResponse = errors.New("processor hasn't received corresponding response for the event")
