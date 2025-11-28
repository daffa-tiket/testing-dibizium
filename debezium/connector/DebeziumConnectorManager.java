package com.tiket.tix.hotel.common.debezium.connector;

import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import com.tiket.tix.hotel.common.debezium.DebeziumHelper;
import com.tiket.tix.hotel.common.debezium.connector.embedded.EmbeddedConnectorRunner;
import com.tiket.tix.hotel.common.debezium.connector.embedded.EmbeddedRunner;
import com.tiket.tix.hotel.common.debezium.connector.kafka.KafkaConsumerRunner;

public class DebeziumConnectorManager {

  private final ConnectorRunner runner;

  public DebeziumConnectorManager(DebeziumConfiguration config) {
    EmbeddedConnectorRunner embeddedRunner = new EmbeddedConnectorRunner(config);

    if (config.getMode() == DebeziumConfiguration.Mode.EMBEDDED) {
      this.runner = new EmbeddedRunner(embeddedRunner);
    } else {
      this.runner = new KafkaConsumerRunner(config, embeddedRunner);
    }
  }

  public void start() throws Exception {
    DebeziumHelper.sendMetric("connector_start");
    runner.start();
  }
}
