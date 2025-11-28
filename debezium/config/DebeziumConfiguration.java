package com.tiket.tix.hotel.common.debezium.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "cdc.debezium")
public class DebeziumConfiguration {
  // Modes: KAFKA (using Kafka Connect), EMBEDDED (embedded Debezium engine)
  // Kafka Connect mode uses Kafka Connect REST API to manage connectors
  // Kafka mode runs Debezium as a separate process via Kafka Connect
  // Kafka mode requires Kafka Connect cluster to be running
  // and accessible via REST API
  // Debezium connectors run as Kafka Connect workers
  // Debezium connectors send change events to Kafka topics
  // Application consumes from Kafka topics
  // Application can use sink connectors or custom consumers
  // Embedded mode runs Debezium engine within the application process
  // Embedded mode directly connects to the source database
  // and captures changes without Kafka
  // Embedded mode applies changes directly to the target database
  // without Kafka
  public enum Mode { KAFKA, EMBEDDED }
  public enum TargetDb { MONGO, POSTGRES }

  // General settings
  // enable or disable Debezium connector
  // default: false
  // if disabled, no Debezium process will run
  // useful for turning off Debezium in certain environments
  private boolean enabled;

  // mode of operation
  // KAFKA or EMBEDDED
  // default: KAFKA
  // using EMBEDDED is not recommended for production
  private Mode mode;

  // Embedded mode
  private String serverName; // unique identifier for embedded

  // Target DB
  // MONGO or POSTGRES
  private TargetDb targetDb;

  // use sink connector
  // if false, use custom consumer
  // means consuming from Kafka topic and applying changes directly
  // to target DB without using Kafka Connect sink connector
  // or with using manual upsert logic
  // if true, deploy sink connector via Kafka Connect REST API
  // default: true
  private boolean useSinkConnector;

  // Kafka configuration
  // used in KAFKA mode and for embedded mode consuming from Kafka
  // to apply changes to target DB
  // includes Kafka Connect REST API URL, topic, bootstrap servers, group ID, poll interval
  // see KafkaConfiguration class for details
  private KafkaConfiguration kafkaConfiguration;

  // Target configuration
  // used for target DB connection details
  // see TargetConfiguration class for details
  private TargetConfiguration targetConfiguration;

  // Source configuration
  // used for source DB connection details
  // see SourceConfiguration class for details
  private SourceConfiguration sourceConfiguration;

  // Worker configuration
  // used for embedded mode worker settings
  // includes local memory settings and realtime listener settings
  // see WorkerConfiguration class for details
  private WorkerConfiguration workerConfiguration;


}
