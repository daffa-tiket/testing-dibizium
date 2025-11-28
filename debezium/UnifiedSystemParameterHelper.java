package com.tiket.tix.hotel.common.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.springframework.stereotype.Component;

@Component
public class UnifiedSystemParameterHelper {
  private static final ConcurrentHashMap<String, Parameter> cache = new ConcurrentHashMap<>();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final DebeziumConfiguration config;
  private final MongoClient mongoClient;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private volatile boolean watcherRunning = false;

  public UnifiedSystemParameterHelper(DebeziumConfiguration config, MongoClient mongoClient) {
    this.config = config;
    this.mongoClient = mongoClient;
  }

  public UnifiedSystemParameterHelper(DebeziumConfiguration config) {
    this.config = config;
    this.mongoClient = null;
  }

  public void refreshCacheFromDb(List<Parameter> allParams) throws IOException {
    for (Parameter p : allParams) {
      cache.put(p.getVariable(), p);
      if (!Objects.isNull(p.getVariable()) && p.getVariable().equalsIgnoreCase("vendorlist")) {
        generateVendorEnum(p);
      }
    }
  }

  private static void generateVendorEnum(Parameter p) {
    try {
      List<String> vendors = (List<String>) getValueAs("vendorList", List.class);

      Path dir = Paths.get("src/main/resources");
      if (!Files.exists(dir)) {
        Files.createDirectories(dir);
      }

      Path out = dir.resolve("vendors.txt");
      if (!Files.exists(out)) {
        Files.createFile(out);
      }

      Files.write(out, vendors);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void startScheduledRefresh() {
    if (!config.getWorkerConfiguration().isEnableLocalMemory()) return;

    // Layer 1: realtime listener / watcher (toggleable)
    if (config.getWorkerConfiguration().isEnableRealtimeListener()) {
      if (config.getTargetDb() == DebeziumConfiguration.TargetDb.MONGO) {
        startMongoWatcher();
      } else if (config.getTargetDb() == DebeziumConfiguration.TargetDb.POSTGRES) {
        startPostgresListener();
      }
    }


    // Layer 2: scheduled polling (fallback)
    scheduler.scheduleAtFixedRate(() -> {
      try {
        List<Parameter> allParams = dbQueryAllParameters();
        refreshCacheFromDb(allParams);
        System.out.println("Parameter cache refreshed (scheduled)");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, config.getWorkerConfiguration().getInitialDelaySec(), config.getWorkerConfiguration()
        .getPeriodSec(), TimeUnit.SECONDS);
  }

  // Layer 1: MongoDB change stream watcher
  // requires replica set
  // watches for changes in the collection
  // updates cache on change
  // uses a single thread executor
  private void startMongoWatcher() {
    if (watcherRunning) return;
    watcherRunning = true;

    Executors.newSingleThreadExecutor().submit(() -> {
      try {
        MongoCollection<Document> collection = mongoClient
            .getDatabase(config.getTargetConfiguration().getMongoDatabase())
            .getCollection(config.getTargetConfiguration().getMongoCollection());

        collection.watch().forEach(change -> {
          try {
            Document doc = change.getFullDocument();
            if (doc != null) {
              String variable = doc.getString("variable");
              String valueStr = doc.getString("value");
              String description = doc.getString("description");
              JsonNode value = objectMapper.readTree(valueStr);
              cache.put(variable, new Parameter(variable, value, description));
              System.out.println("Parameter cache updated via Mongo watcher: " + variable);
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        watcherRunning = false;
      }
    });
  }

  // Layer 1: Postgres listener
  // using LISTEN/NOTIFY
  // requires trigger on unified_system_parameter table to send NOTIFY
  private void startPostgresListener() {
    if (watcherRunning) return;
    watcherRunning = true;

    Executors.newSingleThreadExecutor().submit(() -> {
      try (Connection conn = DriverManager.getConnection(
          config.getTargetConfiguration().getPostgresUrl(),
          config.getTargetConfiguration().getPostgresUser(),
          config.getTargetConfiguration().getPostgresPassword());
          Statement stmt = conn.createStatement()) {

        PGConnection pgConn = conn.unwrap(PGConnection.class);
        stmt.execute("LISTEN unified_parameter_channel");
        System.out.println("Listening to Postgres channel: unified_parameter_channel");

        while (watcherRunning) {
          PGNotification[] notifications = pgConn.getNotifications();
          if (notifications != null) {
            for (PGNotification notification : notifications) {
              String variable = notification.getParameter();
              Parameter updated = dbQueryParameterPostgres(variable);
              if (updated != null) {
                cache.put(variable, updated);
                System.out.println("Parameter cache updated via Postgres listener: " + variable);
              }
            }
          }
          Thread.sleep(500);
        }

      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        watcherRunning = false;
      }
    });
  }

  // helper to query single parameter from Postgres
  // used by Postgres listener
  // updates cache on change
  // uses a single thread executor
  // returns null if not found
  // returns Parameter object if found
  private Parameter dbQueryParameterPostgres(String variable) {
    String sql = "SELECT variable, value, description FROM unified_system_parameter WHERE variable = ?";
    try (Connection conn = DriverManager.getConnection(
        config.getTargetConfiguration().getPostgresUrl(),
        config.getTargetConfiguration().getPostgresUser(),
        config.getTargetConfiguration().getPostgresPassword());
        PreparedStatement ps = conn.prepareStatement(sql)) {

      ps.setString(1, variable);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          String valueStr = rs.getString("value");
          String description = rs.getString("description");
          return new Parameter(variable, objectMapper.readTree(valueStr), description);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public void stopScheduledRefresh() {
    scheduler.shutdownNow();
    watcherRunning = false;
  }

  // get value as JsonNode
  public static JsonNode getValue(String variable) {
    Parameter p = cache.get(variable);
    return p != null ? p.getValue() : null;
  }

  // get value as POJO
  public static <T> T getValueAs(String variable, Class<T> clazz) {
    try {
      JsonNode json = getValue(variable);
      if (json != null) {
        return objectMapper.treeToValue(json, clazz);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  // optional: get description
  public static String getDescription(String variable) {
    Parameter p = cache.get(variable);
    return p != null ? p.getDescription() : null;
  }

  private List<Parameter> dbQueryAllParameters() {
    if (config.getTargetDb() == DebeziumConfiguration.TargetDb.POSTGRES) {
      return dbQueryAllParametersPostgres();
    } else {
      return dbQueryAllParametersMongo();
    }
  }

  public List<Parameter> dbQueryAllParametersPostgres() {
    List<Parameter> list = new ArrayList<>();
    String sql = "SELECT variable, value, description FROM unified_system_parameter";

    try (Connection conn = DriverManager.getConnection(
        config.getTargetConfiguration().getPostgresUrl(),
        config.getTargetConfiguration().getPostgresUser(),
        config.getTargetConfiguration().getPostgresPassword());
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {

      while (rs.next()) {
        String variable = rs.getString("variable");
        String valueStr = rs.getString("value");
        String description = rs.getString("description");
        JsonNode value = objectMapper.readTree(valueStr);

        list.add(new Parameter(variable, value, description));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return list;
  }

  public List<Parameter> dbQueryAllParametersMongo() {
    List<Parameter> list = new ArrayList<>();
    MongoCollection<Document> collection = mongoClient
        .getDatabase(config.getTargetConfiguration().getMongoDatabase())
        .getCollection(config.getTargetConfiguration().getMongoCollection());

    try (MongoCursor<Document> cursor = collection.find().iterator()) {
      while (cursor.hasNext()) {
        Document doc = cursor.next();
        String variable = doc.getString("variable");
        String valueStr = doc.getString("value");
        String description = doc.getString("description");
        JsonNode value = objectMapper.readTree(valueStr);

        list.add(new Parameter(variable, value, description));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return list;
  }
}
