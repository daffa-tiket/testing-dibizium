package persistent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/helper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoUpsertHandler struct {
	Client *mongo.Client
	cfg    *config.DebeziumConfig
	col    *mongo.Collection
}

func NewMongoUpsertHandler(cfg *config.DebeziumConfig) *MongoUpsertHandler {
	client, _ := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.TargetConfig.MongoURI))
	db := client.Database(cfg.TargetConfig.MongoDB)
	col := db.Collection(cfg.TargetConfig.MongoCol)

	h := &MongoUpsertHandler{
		Client: client,
		cfg:    cfg,
		col:    col,
	}

	h.migrateMongo()
	return h
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

	filter := bson.M{"_id": id}
	opts := options.Replace().SetUpsert(true)

	_, err := h.col.ReplaceOne(ctx, filter, after, opts)
	if err != nil {
		helper.SendMetric(helper.TagBuilder{
			EventType: "mongo_upsert_failed",
		})
		return err
	}

	helper.SendMetric(helper.TagBuilder{
		EventType: "mongo_upsert_success",
	})
	return nil
}

func (h *MongoUpsertHandler) migrateMongo() {
	ctx := context.Background()

	indexModel := mongo.IndexModel{
		Keys:    bson.M{"variable": 1},
		Options: options.Index().SetUnique(true),
	}

	if _, err := h.col.Indexes().CreateOne(ctx, indexModel); err != nil {
		log.Println("Mongo create index failed:", err)
		return
	}

	fmt.Println("Mongo migration done.")
}