package com.tiket.tix.hotel.common.debezium.connector.sink;

import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;

public class KafkaSinkConfigBuilder {

  public static String buildPostgresSink(DebeziumConfiguration config) {
    return "{\n" +
        "  \"name\": \"postgres-sink-connector\",\n" +
        "  \"config\": {\n" +
        "    \"connector.class\": \"io.confluent.connect.jdbc.JdbcSinkConnector\",\n" +
        "    \"tasks.max\": \"1\",\n" +
        "    \"connection.url\": \"" + config.getTargetConfiguration().getPostgresUrl() + "\",\n" +
        "    \"connection.user\": \"" + config.getTargetConfiguration().getPostgresUser() + "\",\n" +
        "    \"connection.password\": \"" + config.getTargetConfiguration().getPostgresPassword() + "\",\n" +
        "    \"topics\": \"" + config.getKafkaConfiguration().getTopic() + "\",\n" +
        "    \"auto.create\": \"true\",\n" +
        "    \"auto.evolve\": \"true\",\n" +
        "    \"insert.mode\": \"upsert\",\n" +
        "    \"pk.mode\": \"record_key\",\n" +
        "    \"pk.fields\": \"id\"\n" +
        "  }\n" +
        "}";
  }

  public static String buildMongoSink(DebeziumConfiguration config) {
    return "{\n" +
        "  \"name\": \"mongo-sink-connector\",\n" +
        "  \"config\": {\n" +
        "    \"connector.class\": \"com.mongodb.kafka.connect.MongoSinkConnector\",\n" +
        "    \"tasks.max\": \"1\",\n" +
        "    \"connection.uri\": \"" + config.getTargetConfiguration().getMongoUri() + "\",\n" +
        "    \"database\": \"" + config.getTargetConfiguration().getMongoDatabase() + "\",\n" +
        "    \"collection\": \"" + config.getTargetConfiguration().getMongoCollection() + "\",\n" +
        "    \"topics\": \"" + config.getKafkaConfiguration().getTopic() + "\",\n" +
        "    \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",\n" +
        "    \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",\n" +
        "    \"value.converter.schemas.enable\": \"false\",\n" +
        "    \"writemodel.strategy\": \"com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy\"\n" +
        "  }\n" +
        "}";
  }
}
