package embedded

import (
	"database/sql"
	"fmt"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/persistent"
	"go.mongodb.org/mongo-driver/mongo"
)

type UpsertConnectorRunner struct {
	cfg           *config.DebeziumConfig
	UpsertHandler persistent.UpsertHandler
	Client        *mongo.Client
	Db           *sql.DB
}

func NewUpsertConnectorRunner(cfg *config.DebeziumConfig) *UpsertConnectorRunner {
	runner := &UpsertConnectorRunner{cfg: cfg}

	if cfg.TargetDB == "MONGO" {
		upsertHandler := persistent.NewMongoUpsertHandler(cfg)
		runner.UpsertHandler = upsertHandler
		runner.Client = upsertHandler.Client
	} else {
		upsertHandler := persistent.NewPostgresUpsertHandler(cfg)
		runner.UpsertHandler = upsertHandler
		runner.Db = upsertHandler.Db
	}

	return runner
}

func (r *UpsertConnectorRunner) HandleChangeFromJSON(jsonStr string) {
	r.UpsertHandler.Upsert(jsonStr)
}

func (r *UpsertConnectorRunner) StartEmbedded() {
	fmt.Println("Debezium Embedded Engine started")

}
