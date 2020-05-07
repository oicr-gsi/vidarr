package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class OutputEntry {

  private ObjectNode key;
  private Map<String, OutputAssociation> outputs;

  public ObjectNode getKey() {
    return key;
  }

  public Map<String, OutputAssociation> getOutputs() {
    return outputs;
  }

  public void setKey(ObjectNode key) {
    this.key = key;
  }

  public void setOutputs(Map<String, OutputAssociation> outputs) {
    this.outputs = outputs;
  }
}
