package kafka

import (
	"fmt"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/config"
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
		fmt.Println("sink_connector_started_use_postgres")
	case "MONGO":
		jsonConfig = BuildMongoSink(s.cfg)
		if err := client.RegisterConnector(kafkaConnectURL, "mongo-sink-connector", jsonConfig); err != nil {
			return err
		}
		fmt.Println("sink_connector_started_use_mongo")
	}

	return nil
}
