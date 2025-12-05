package connector

import (
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/connector/embedded"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/connector/kafka"
)

type DebeziumConnectorManager struct {
	runner ConnectorRunner
}

func NewDebeziumConnectorManager(cfg *config.DebeziumConfig) *DebeziumConnectorManager {
	embeddedConnectorRunner := embedded.NewEmbeddedConnectorRunner(cfg)

	var runner ConnectorRunner
	if cfg.Mode == "EMBEDDED" {
		runner = embedded.NewEmbeddedRunner(embeddedConnectorRunner)
	} else {
		runner = kafka.NewKafkaConsumerRunner(cfg, embeddedConnectorRunner)
	}

	return &DebeziumConnectorManager{runner: runner}
}

func (m *DebeziumConnectorManager) Start() error {
	println("connector_start")

	return m.runner.Start()
}