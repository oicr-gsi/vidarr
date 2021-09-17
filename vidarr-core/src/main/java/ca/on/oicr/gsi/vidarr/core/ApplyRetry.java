package ca.on.oicr.gsi.vidarr.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.function.Consumer;

final class ApplyRetry implements Consumer<ObjectNode> {
  private final List<JsonPath> jsonPath;
  private final JsonNode value;

  public ApplyRetry(List<JsonPath> jsonPath, JsonNode value) {
    this.jsonPath = jsonPath;
    this.value = value;
  }

  @Override
  public void accept(ObjectNode objectNode) {
    JsonNode current = objectNode;
    for (var i = 0; i < jsonPath.size() - 1; i++) {
      current = jsonPath.get(i).get(current);
    }
    jsonPath.get(jsonPath.size() - 1).set(current, value);
  }
}
