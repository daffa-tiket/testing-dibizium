package com.tiket.tix.hotel.common.debezium.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class KafkaConfiguration {
  private String connectUrl;
  private String topic;
  private String bootstrapServers;
  private String groupId;
  private long pollIntervalMs;
}
