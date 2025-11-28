package com.tiket.tix.hotel.common.debezium;

import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import com.tiket.tix.hotel.common.debezium.config.KafkaConfiguration;
import com.tiket.tix.hotel.common.debezium.connector.DebeziumConnectorManager;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DebeziumAutoStarter {

  @Autowired
  private DebeziumConfiguration props;

  @PostConstruct
  public void init() throws Exception {

    if (!props.isEnabled()) {
      System.out.println("Debezium disabled via properties, skipping startup");
      return;
    }

    DebeziumConnectorManager manager = new DebeziumConnectorManager(props);
    manager.start();
  }
}
