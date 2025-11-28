package com.tiket.tix.hotel.common.debezium.connector.embedded;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import com.tiket.tix.hotel.common.debezium.DebeziumHelper;
import com.tiket.tix.hotel.common.debezium.UnifiedSystemParameterHelper;
import com.tiket.tix.hotel.common.debezium.persistent.MongoUpsertHandler;
import com.tiket.tix.hotel.common.debezium.persistent.PostgresUpsertHandler;
import com.tiket.tix.hotel.common.debezium.persistent.UpsertHandler;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EmbeddedConnectorRunner {
  private final DebeziumConfiguration config;
  UpsertHandler upsertHandler;

  public EmbeddedConnectorRunner(DebeziumConfiguration config) {
    this.config = config;

    if (config.getTargetDb() == DebeziumConfiguration.TargetDb.MONGO) {
      MongoClient mongoClient = MongoClients.create(config.getTargetConfiguration().getMongoUri());
      UnifiedSystemParameterHelper helper = new UnifiedSystemParameterHelper(config, mongoClient);
      this.upsertHandler = new MongoUpsertHandler(config, mongoClient);
      helper.startScheduledRefresh();
    } else {
      UnifiedSystemParameterHelper helper = new UnifiedSystemParameterHelper(config);
      this.upsertHandler = new PostgresUpsertHandler(config);
      helper.startScheduledRefresh();
    }
  }

  public void handleChange(SourceRecord record) {
    DebeziumHelper.sendMetric("embedded_record_received");
    try {
      upsertHandler.upsert(record.value().toString());
      DebeziumHelper.sendMetric("embedded_record_upserted");
    } catch (Exception e) {
      DebeziumHelper.sendMetric("embedded_record_failed");
    }
  }

  public void handleChangeFromJson(String json) {
    upsertHandler.upsert(json);
  }

  public void startEmbedded() {
    Configuration cfg = Configuration.create()
        .with("name", config.getServerName())
        .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
        .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
        .with("database.hostname", config.getSourceConfiguration().getPostgresHost())
        .with("database.port", String.valueOf(config.getSourceConfiguration().getPostgresPort()))
        .with("database.user", config.getSourceConfiguration().getPostgresUser())
        .with("database.password", config.getSourceConfiguration().getPostgresPassword())
        .with("database.dbname", config.getSourceConfiguration().getPostgresDbName())
        .with("plugin.name", "pgoutput")
        .with("slot.name", config.getSourceConfiguration().getSlotName())
        .with("table.include.list", config.getSourceConfiguration().getPostgresTableIncludeList())
        .with("database.server.name", config.getServerName())
        .build();

    EmbeddedEngine engine = EmbeddedEngine.create()
        .using(cfg)
        .notifying(this::handleChange)
        .build();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(engine);
    System.out.println("Debezium Embedded Engine started");
  }
}
