package kafka

import (
	"fmt"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/connector/embedded"
)

type KafkaConsumerRunner struct {
	cfg    *config.DebeziumConfig
	runner *embedded.EmbeddedConnectorRunner
}

func NewKafkaConsumerRunner(cfg *config.DebeziumConfig, runner *embedded.EmbeddedConnectorRunner) *KafkaConsumerRunner {
	return &KafkaConsumerRunner{cfg: cfg, runner: runner}
}

func (k *KafkaConsumerRunner) Start() error {
	if k.cfg.UseSinkConnector {
		NewSinkConnectorRunner(k.cfg).Start()
	} else {
		consumer := NewDebeziumKafkaConsumer(k.cfg, k.runner)
		go consumer.StartPolling()
		fmt.Println("connector_started_use_kafka")
	}
	return nil
}
