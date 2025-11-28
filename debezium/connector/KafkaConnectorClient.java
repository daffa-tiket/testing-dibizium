package com.tiket.tix.hotel.common.debezium.connector;

import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class KafkaConnectorClient {
  private final OkHttpClient client = new OkHttpClient();

  public void registerConnector(String kafkaConnectUrl, String connectorName, String jsonConfig) throws IOException {
    RequestBody body = RequestBody.create(MediaType.get("application/json"), jsonConfig);
    Request request = new Request.Builder()
        .url(kafkaConnectUrl + "/connectors")
        .post(body)
        .build();

    try (Response response = client.newCall(request).execute()) {
      if (response.isSuccessful()) {
        System.out.println("Connector " + connectorName + " registered");
      } else {
        System.err.println("Failed to register connector " + connectorName + ": " + response.body().string());
      }
    }
  }
}
