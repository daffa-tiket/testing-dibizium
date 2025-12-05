package embedded

import (
	"fmt"
)

type EmbeddedRunner struct {
	runner *EmbeddedConnectorRunner
}

func NewEmbeddedRunner(runner *EmbeddedConnectorRunner) *EmbeddedRunner {
	return &EmbeddedRunner{runner: runner}
}

func (e *EmbeddedRunner) Start() error {
	e.runner.StartEmbedded()
	// metric placeholder
	fmt.Println("connector_started_use_embedded")
	return nil
}
