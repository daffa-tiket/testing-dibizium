package com.tiket.tix.hotel.common.debezium.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class SourceConfiguration {
  // Postgres source
  private String postgresHost;
  private int postgresPort;
  private String postgresUser;
  private String postgresPassword;
  private String postgresDbName;
  private String postgresTableIncludeList;
  private String slotName;
}
