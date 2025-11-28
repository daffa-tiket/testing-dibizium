package com.tiket.tix.hotel.common.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

@Data
public class Parameter {
  private String variable;
  private JsonNode value; // JSON object
  private String description;

  public Parameter(String variable, JsonNode value, String description) {
    this.variable = variable;
    this.value = value;
    this.description = description;
  }
}