package com.tiket.tix.hotel.common.debezium.connector.kafka;

import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import com.tiket.tix.hotel.common.debezium.DebeziumHelper;
import com.tiket.tix.hotel.common.debezium.connector.ConnectorRunner;
import com.tiket.tix.hotel.common.debezium.connector.sink.SinkConnectorRunner;
import com.tiket.tix.hotel.common.debezium.connector.embedded.EmbeddedConnectorRunner;

public class KafkaConsumerRunner implements ConnectorRunner {

  private final DebeziumConfiguration config;
  private final EmbeddedConnectorRunner runner;

  public KafkaConsumerRunner(DebeziumConfiguration config, EmbeddedConnectorRunner runner) {
    this.config = config;
    this.runner = runner;
  }

  @Override
  public void start() {
    if (config.isUseSinkConnector()) {
      new SinkConnectorRunner(config).start();
    } else {
      DebeziumKafkaConsumer consumer = new DebeziumKafkaConsumer(config, runner);
      consumer.startPolling();
      DebeziumHelper.sendMetric("connector_started_use_kafka");
    }
  }
}
