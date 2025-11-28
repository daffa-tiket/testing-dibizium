package com.tiket.tix.hotel.common.debezium.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class TargetConfiguration {
  // Mongo target
  // mongodb://user:password@host:port/dbname
  private String mongoUri;

  // mongo target database and collection
  private String mongoDatabase;
  private String mongoCollection;

  // Postgres target
  // postgresql://host:port/dbname
  private String postgresUrl;

  // postgresql target credentials
  private String postgresUser;
  private String postgresPassword;

  // postgres target table
  private String postgresTable;
}
