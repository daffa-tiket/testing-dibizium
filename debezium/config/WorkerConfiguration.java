package com.tiket.tix.hotel.common.debezium.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class WorkerConfiguration {
  // Worker Local Memory
  private boolean enableLocalMemory;
  private long initialDelaySec;
  private long periodSec;

  // Realtime listener (Postres Listen / Notify or Mongo Watcher)
  private boolean enableRealtimeListener;
}
