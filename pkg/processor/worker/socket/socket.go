package socket

import (
	"github.com/nuclio/logger"
	"github.com/nuclio/nuclio-sdk-go"
)

type ASocket interface {
	// ProcessEvent receives the event and processes it at the specific runtime
	ProcessEvent(event nuclio.Event, functionLogger logger.Logger) (interface{}, error)

	// ProcessBatch receives the event batch and processes it at the specific runtime
	ProcessBatch(batch []nuclio.Event, functionLogger logger.Logger) ([]*ResponseWithErrors, error)
}

type Socket struct {
	//worker *Worker
}
