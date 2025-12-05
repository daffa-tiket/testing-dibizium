package connector

import (
	"database/sql"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/connector/embedded"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/connector/kafka"
	"go.mongodb.org/mongo-driver/mongo"
)

type DebeziumConnectorManager struct {
	runner ConnectorRunner
	Client        *mongo.Client
	Db           *sql.DB
}

func NewDebeziumConnectorManager(cfg *config.DebeziumConfig) *DebeziumConnectorManager {
	embeddedConnectorRunner := embedded.NewUpsertConnectorRunner(cfg)


	var runner ConnectorRunner
	if cfg.Mode == "EMBEDDED" {
		runner = embedded.NewEmbeddedRunner(embeddedConnectorRunner)
	} else {
		runner = kafka.NewKafkaConsumerRunner(cfg, embeddedConnectorRunner)
	}

	return &DebeziumConnectorManager{runner: runner, Client: embeddedConnectorRunner.Client, Db: embeddedConnectorRunner.Db}
}

func (m *DebeziumConnectorManager) Start() error {
	println("connector_start")

	return m.runner.Start()
}
