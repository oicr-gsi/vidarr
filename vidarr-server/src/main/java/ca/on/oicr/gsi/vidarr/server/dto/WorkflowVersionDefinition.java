package ca.on.oicr.gsi.vidarr.server.dto;

import java.util.Map;

public final class WorkflowVersionDefinition {
  private Map<String, String> parameters;

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }
}
