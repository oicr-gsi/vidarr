package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

/** The current state of a running workflow to be recorded in the database */
public final class EngineState {
  private String cromwellId;
  private JsonNode engineParameters;
  private ObjectNode parameters;
  private String vidarrId;
  private Map<String, String> workflowInputFiles;
  private WorkflowLanguage workflowLanguage;
  private String workflowSource;

  public String getCromwellId() {
    return cromwellId;
  }

  public JsonNode getEngineParameters() {
    return engineParameters;
  }

  public ObjectNode getParameters() {
    return parameters;
  }

  public String getVidarrId() {
    return vidarrId;
  }

  public Map<String, String> getWorkflowInputFiles() {
    return workflowInputFiles;
  }

  public WorkflowLanguage getWorkflowLanguage() {
    return workflowLanguage;
  }

  public String getWorkflowSource() {
    return workflowSource;
  }

  public void setCromwellId(String cromwellId) {
    this.cromwellId = cromwellId;
  }

  public void setEngineParameters(JsonNode engineParameters) {
    this.engineParameters = engineParameters;
  }

  public void setParameters(ObjectNode parameters) {
    this.parameters = parameters;
  }

  public void setVidarrId(String vidarrId) {
    this.vidarrId = vidarrId;
  }

  public void setWorkflowInputFiles(Map<String, String> workflowInputFiles) {
    this.workflowInputFiles = workflowInputFiles;
  }

  public void setWorkflowLanguage(WorkflowLanguage workflowLanguage) {
    this.workflowLanguage = workflowLanguage;
  }

  public void setWorkflowSource(String workflowSource) {
    this.workflowSource = workflowSource;
  }
}
