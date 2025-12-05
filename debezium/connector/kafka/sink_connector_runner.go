package kafka

import (
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/helper"
)

type SinkConnectorRunner struct {
	cfg *config.DebeziumConfig
}

func NewSinkConnectorRunner(cfg *config.DebeziumConfig) *SinkConnectorRunner {
	return &SinkConnectorRunner{cfg: cfg}
}

func (s *SinkConnectorRunner) Start() error {
	client := NewKafkaConnectorClient()
	kafkaConnectURL := s.cfg.KafkaConfig.ConnectURL

	var jsonConfig string
	switch s.cfg.TargetDB {
	case "POSTGRES":
		jsonConfig = BuildPostgresSink(s.cfg)
		if err := client.RegisterConnector(kafkaConnectURL, "postgres-sink-connector", jsonConfig); err != nil {
			return err
		}
		helper.SendMetric(helper.TagBuilder{
			EventType: "sink_connector_started_use_postgres",
		})
	case "MONGO":
		jsonConfig = BuildMongoSink(s.cfg)
		if err := client.RegisterConnector(kafkaConnectURL, "mongo-sink-connector", jsonConfig); err != nil {
			return err
		}
		helper.SendMetric(helper.TagBuilder{
			EventType: "sink_connector_started_use_mongo",
		})
	}

	return nil
}
