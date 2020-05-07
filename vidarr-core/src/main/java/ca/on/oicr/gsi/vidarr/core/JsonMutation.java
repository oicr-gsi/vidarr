package ca.on.oicr.gsi.vidarr.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Describe a change to a JSON document */
public final class JsonMutation {
  private List<JsonPath> path;
  private JsonNode result;

  public JsonMutation() {}

  public JsonMutation(JsonNode result, Stream<JsonPath> path) {
    this.result = result;
    this.path = path.collect(Collectors.toList());
  }

  public List<JsonPath> getPath() {
    return path;
  }

  public JsonNode getResult() {
    return result;
  }

  public void setPath(List<JsonPath> path) {
    this.path = path;
  }

  public void setResult(JsonNode result) {
    this.result = result;
  }
}
