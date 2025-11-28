package com.tiket.tix.hotel.common.debezium.persistent;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReplaceOptions;
import com.tiket.tix.hotel.common.debezium.config.DebeziumConfiguration;
import com.tiket.tix.hotel.common.debezium.DebeziumHelper;
import java.util.ArrayList;
import org.bson.Document;

public class MongoUpsertHandler implements UpsertHandler {
  private final MongoClient mongoClient;
  private final DebeziumConfiguration config;

  public MongoUpsertHandler(DebeziumConfiguration config, MongoClient mongoClient) {
    this.config = config;
    this.mongoClient = mongoClient;
    migrateMongo(mongoClient);
  }

  @Override
  public void upsert(String json) {
    try {
      Document root = Document.parse(json);
      Document payload = (Document) root.get("payload");
      Document after = payload != null ? (Document) payload.get("after") : null;

      if (after != null) {
        Object id = after.get("id");
        MongoDatabase db = mongoClient.getDatabase(config.getTargetConfiguration().getMongoDatabase());
        MongoCollection<Document> collection = db.getCollection(config.getTargetConfiguration().getMongoCollection());
        collection.replaceOne(
            new Document("_id", id),
            after,
            new ReplaceOptions().upsert(true)
        );
      }
      DebeziumHelper.sendMetric("mongo_upsert_success");
    } catch (Exception e) {
      DebeziumHelper.sendMetric("mongo_upsert_failed");
      e.printStackTrace();
    }
  }


  private void migrateMongo(MongoClient mongoClients) {
    try {
      MongoDatabase db = mongoClients.getDatabase(config.getTargetConfiguration().getMongoDatabase());
      if (!db.listCollectionNames().into(new ArrayList<>())
          .contains(config.getTargetConfiguration().getMongoCollection())) {
        db.createCollection(config.getTargetConfiguration().getMongoCollection());
        db.getCollection(config.getTargetConfiguration().getMongoCollection())
            .createIndex(Indexes.ascending("variable"), new IndexOptions().unique(true));
        System.out.println("Mongo migration done.");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
