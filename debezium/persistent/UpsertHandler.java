package com.tiket.tix.hotel.common.debezium.persistent;

public interface UpsertHandler {
  void upsert(String json);
}
