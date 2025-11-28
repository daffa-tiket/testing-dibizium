package com.tiket.tix.hotel.common.debezium.connector.sink;

import com.tiket.tix.hotel.common.debezium.DebeziumHelper;
import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import com.tiket.tix.hotel.common.debezium.connector.ConnectorRunner;
import com.tiket.tix.hotel.common.debezium.connector.KafkaConnectorClient;

public class SinkConnectorRunner implements ConnectorRunner {

  private final DebeziumConfiguration config;

  public SinkConnectorRunner(DebeziumConfiguration config) {
    this.config = config;
  }

  @Override
  public void start() {
    try {
      KafkaConnectorClient client = new KafkaConnectorClient();
      String kafkaConnectUrl = config.getKafkaConfiguration().getConnectUrl();

      if (config.getTargetDb() == DebeziumConfiguration.TargetDb.POSTGRES) {
        String json = KafkaSinkConfigBuilder.buildPostgresSink(config);
        client.registerConnector(kafkaConnectUrl, "postgres-sink-connector", json);
        DebeziumHelper.sendMetric("sink_connector_started_use_postgres");

      } else if (config.getTargetDb() == DebeziumConfiguration.TargetDb.MONGO) {
        String json = KafkaSinkConfigBuilder.buildMongoSink(config);
        client.registerConnector(kafkaConnectUrl, "mongo-sink-connector", json);
        DebeziumHelper.sendMetric("sink_connector_started_use_mongo");
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
