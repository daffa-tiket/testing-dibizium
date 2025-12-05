package persistent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/internal/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoUpsertHandler struct {
	client *mongo.Client
	cfg    *config.DebeziumConfig
	col    *mongo.Collection
}

func NewMongoUpsertHandler(cfg *config.DebeziumConfig, client *mongo.Client) *MongoUpsertHandler {
	handler := &MongoUpsertHandler{
		client: client,
		cfg:    cfg,
	}
	handler.migrateMongo()
	return handler
}

func (h *MongoUpsertHandler) Upsert(jsonStr string) error {
	var root map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &root); err != nil {
		log.Println("Mongo upsert failed: unmarshal error", err)
		return err
	}

	payload, ok := root["payload"].(map[string]interface{})
	if !ok {
		return nil
	}
	after, ok := payload["after"].(map[string]interface{})
	if !ok || after == nil {
		return nil
	}

	id := after["id"]
	ctx := context.Background()
	db := h.client.Database(h.cfg.TargetConfig.MongoDB)
	collection := db.Collection(h.cfg.TargetConfig.MongoCol)

	filter := bson.M{"_id": id}
	opts := options.Replace().SetUpsert(true)

	_, err := collection.ReplaceOne(ctx, filter, after, opts)
	if err != nil {
		log.Println("Mongo upsert failed:", err)
		return err
	}

	// metric placeholder
	fmt.Println("mongo_upsert_success")
	return nil
}

func (h *MongoUpsertHandler) migrateMongo() {
	ctx := context.Background()
	db := h.client.Database(h.cfg.TargetConfig.MongoDB)
	collection := db.Collection(h.cfg.TargetConfig.MongoCol)

	indexModel := mongo.IndexModel{
		Keys:    bson.M{"variable": 1},
		Options: options.Index().SetUnique(true),
	}

	_, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		log.Println("Mongo create index failed:", err)
		return
	}

	fmt.Println("Mongo migration done (collection auto-created if missing, index ensured).")
}