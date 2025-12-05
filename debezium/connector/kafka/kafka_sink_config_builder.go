package kafka

import (
	"fmt"

	"github.com/tiket/TIX-HOTEL-UTILITIES-GO/debezium/config"
)

func BuildPostgresSink(cfg *config.DebeziumConfig) string {
	return fmt.Sprintf(`{
  "name": "postgres-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "%s",
    "connection.user": "%s",
    "connection.password": "%s",
    "topics": "%s",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "id"
  }
}`, cfg.TargetConfig.PostgresURL,
		cfg.TargetConfig.PostgresUser,
		cfg.TargetConfig.PostgresPass,
		cfg.KafkaConfig.Topic)
}

func BuildMongoSink(cfg *config.DebeziumConfig) string {
	return fmt.Sprintf(`{
  "name": "mongo-sink-connector",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "tasks.max": "1",
    "connection.uri": "%s",
    "database": "%s",
    "collection": "%s",
    "topics": "%s",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"
  }
}`, cfg.TargetConfig.MongoURI,
		cfg.TargetConfig.MongoDB,
		cfg.TargetConfig.MongoCol,
		cfg.KafkaConfig.Topic)
}
