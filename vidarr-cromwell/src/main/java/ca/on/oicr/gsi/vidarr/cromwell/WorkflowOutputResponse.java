package ca.on.oicr.gsi.vidarr.cromwell;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** The Cromwell response when the output for the workflow is requested */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WorkflowOutputResponse {
  private String id;
  private ObjectNode outputs;

  public String getId() {
    return id;
  }

  public ObjectNode getOutputs() {
    return outputs;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setOutputs(ObjectNode outputs) {
    this.outputs = outputs;
  }
}
