package ca.on.oicr.gsi.vidarr.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/** Describe a change to a JSON document */
public record JsonMutation(List<JsonPath> path, JsonNode result) {

  public List<JsonPath> getPath() {
    return path;
  }

  public JsonNode getResult() {
    return result;
  }
}
