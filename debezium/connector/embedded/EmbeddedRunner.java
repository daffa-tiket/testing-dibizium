package com.tiket.tix.hotel.common.debezium.connector.embedded;

import com.tiket.tix.hotel.common.debezium.DebeziumHelper;
import com.tiket.tix.hotel.common.debezium.connector.ConnectorRunner;

public class EmbeddedRunner implements ConnectorRunner {

  private final EmbeddedConnectorRunner runner;

  public EmbeddedRunner(EmbeddedConnectorRunner runner) {
    this.runner = runner;
  }

  @Override
  public void start() {
    runner.startEmbedded();
    DebeziumHelper.sendMetric("connector_started_use_embedded");
  }
}
