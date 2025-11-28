package com.tiket.tix.hotel.common.debezium.connector.kafka;

import com.tiket.tix.hotel.common.debezium.DebeziumHelper;
import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import com.tiket.tix.hotel.common.debezium.connector.embedded.EmbeddedConnectorRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DebeziumKafkaConsumer {
  private final KafkaConsumer<String, String> consumer;
  private final EmbeddedConnectorRunner runner;
  private final long durationPollMillis;

  public DebeziumKafkaConsumer(DebeziumConfiguration config, EmbeddedConnectorRunner runner) {
    this.runner = runner;

    Properties props = new Properties();
    props.put("bootstrap.servers", config.getKafkaConfiguration().getBootstrapServers());
    props.put("group.id", config.getKafkaConfiguration().getGroupId());
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    this.durationPollMillis = config.getKafkaConfiguration().getPollIntervalMs();
    this.consumer = new KafkaConsumer<>(props);
    this.consumer.subscribe(Collections.singletonList(config.getKafkaConfiguration().getTopic()));
  }

  public void startPolling() {
    new Thread(() -> {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(durationPollMillis));
        for (ConsumerRecord<String, String> record : records) {
          DebeziumHelper.sendMetric("kafka_consumer_received");
          try {
            runner.handleChangeFromJson(record.value());
            DebeziumHelper.sendMetric("kafka_consumer_processed");
          } catch (Exception e) {
            DebeziumHelper.sendMetric("kafka_consumer_failed");
          }
        }
      }
    }).start();
  }
}
