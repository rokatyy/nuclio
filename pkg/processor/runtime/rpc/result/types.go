package result

import "github.com/nuclio/nuclio-sdk-go"

type ResultWithNuclioProcessingResult interface {
	Result
	GetProcessingResult() nuclio.ProcessingResult
}

type Result interface {
	IsStream() bool

	Error() error
}
