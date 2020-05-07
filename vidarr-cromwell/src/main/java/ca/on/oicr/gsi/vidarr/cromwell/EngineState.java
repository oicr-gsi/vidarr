package ca.on.oicr.gsi.vidarr.cromwell;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** The current state of a running workflow to be recorded in the database */
public final class EngineState {
  private String cromwellId;
  private ObjectNode engineParameters;
  private ObjectNode parameters;
  private String workflowUrl;

  public String getCromwellId() {
    return cromwellId;
  }

  public ObjectNode getEngineParameters() {
    return engineParameters;
  }

  public ObjectNode getParameters() {
    return parameters;
  }

  public String getWorkflowUrl() {
    return workflowUrl;
  }

  public void setCromwellId(String cromwellId) {
    this.cromwellId = cromwellId;
  }

  public void setEngineParameters(ObjectNode engineParameters) {
    this.engineParameters = engineParameters;
  }

  public void setParameters(ObjectNode parameters) {
    this.parameters = parameters;
  }

  public void setWorkflowUrl(String workflowUrl) {
    this.workflowUrl = workflowUrl;
  }
}
