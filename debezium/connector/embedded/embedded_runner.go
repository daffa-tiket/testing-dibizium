package embedded

import (
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/helper"
)

type EmbeddedRunner struct {
	runner *UpsertConnectorRunner
}

func NewEmbeddedRunner(runner *UpsertConnectorRunner) *EmbeddedRunner {
	return &EmbeddedRunner{runner: runner}
}

func (e *EmbeddedRunner) Start() error {
	e.runner.StartEmbedded()
	// metric placeholder
	helper.SendMetric(helper.TagBuilder{
		EventType: "connector_started_use_embedded",
	})
	return nil
}
