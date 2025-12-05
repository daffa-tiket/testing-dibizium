package kafka

import (

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/connector/embedded"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/helper"
)

type KafkaConsumerRunner struct {
	cfg    *config.DebeziumConfig
	runner *embedded.UpsertConnectorRunner
}

func NewKafkaConsumerRunner(cfg *config.DebeziumConfig, runner *embedded.UpsertConnectorRunner) *KafkaConsumerRunner {
	return &KafkaConsumerRunner{cfg: cfg, runner: runner}
}

func (k *KafkaConsumerRunner) Start() error {
	if k.cfg.UseSinkConnector {
		NewSinkConnectorRunner(k.cfg).Start()
	} else {
		consumer := NewDebeziumKafkaConsumer(k.cfg, k.runner)
		go consumer.StartPolling()
		helper.SendMetric(helper.TagBuilder{
			EventType: "connector_started_use_kafka",
		})
	}
	return nil
}
