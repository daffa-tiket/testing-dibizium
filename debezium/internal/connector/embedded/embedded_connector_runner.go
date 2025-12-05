package embedded

import (
	"context"
	"fmt"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/persistent"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EmbeddedConnectorRunner struct {
	cfg          *config.DebeziumConfig
	UpsertHandler persistent.UpsertHandler
}

func NewEmbeddedConnectorRunner(cfg *config.DebeziumConfig) *EmbeddedConnectorRunner {
	runner := &EmbeddedConnectorRunner{cfg: cfg}

	if cfg.TargetDB == "MONGO" {
		client, _ := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.TargetConfig.MongoURI))
		runner.UpsertHandler = persistent.NewMongoUpsertHandler(cfg, client)
	} else {
		runner.UpsertHandler = persistent.NewPostgresUpsertHandler(cfg)
	}

	return runner
}

func (r *EmbeddedConnectorRunner) HandleChangeFromJSON(jsonStr string) {
	r.UpsertHandler.Upsert(jsonStr)
}

func (r *EmbeddedConnectorRunner) StartEmbedded() {
	fmt.Println("Debezium Embedded Engine started")

}
