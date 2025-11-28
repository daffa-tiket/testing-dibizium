package com.tiket.tix.hotel.common.debezium.persistent;

import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import com.tiket.tix.hotel.common.debezium.DebeziumHelper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.sql.Statement;

public class PostgresUpsertHandler implements UpsertHandler {

  private final DebeziumConfiguration config;
  private final Gson gson = new Gson();

  public PostgresUpsertHandler(DebeziumConfiguration config) {
    this.config = config;
    migratePostgres();
  }

  @Override
  public void upsert(String json) {
    try {
      JsonObject root = gson.fromJson(json, JsonObject.class);
      JsonObject payload = root.getAsJsonObject("payload");
      JsonObject after = payload != null && payload.has("after")
          ? payload.getAsJsonObject("after")
          : null;
      if (after == null) return;

      try (Connection conn = DriverManager.getConnection(
          config.getTargetConfiguration().getPostgresUrl(),
          config.getTargetConfiguration().getPostgresUser(),
          config.getTargetConfiguration().getPostgresPassword())) {

        String sql = "INSERT INTO unified_system_parameter (variable, value, description) " +
            "VALUES (?, ?, ?) " +
            "ON CONFLICT (variable) DO UPDATE SET value = EXCLUDED.value, description = EXCLUDED.description";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
          ps.setString(1, after.get("variable").getAsString());
          // parse value string JSON jadi string biasa
          ps.setString(2, after.get("value").getAsString());
          ps.setString(3, after.has("description") && !after.get("description").isJsonNull()
              ? after.get("description").getAsString()
              : null);
          ps.executeUpdate();
        }
      }
      DebeziumHelper.sendMetric("postgres_upsert_success");
    } catch (Exception e) {
      DebeziumHelper.sendMetric("postgres_upsert_failed");
      e.printStackTrace();
    }
  }


  private void migratePostgres() {
    try (Connection conn = DriverManager.getConnection(
        config.getTargetConfiguration().getPostgresUrl(),
        config.getTargetConfiguration().getPostgresUser(),
        config.getTargetConfiguration().getPostgresPassword());
        Statement stmt = conn.createStatement()) {

      String sql = "CREATE TABLE IF NOT EXISTS unified_system_parameter (" +
          "variable VARCHAR(255) PRIMARY KEY," +
          "value TEXT NOT NULL," +
          "description TEXT" +
          ")";
      stmt.execute(sql);
      System.out.println("Postgres migration done.");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
