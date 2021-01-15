package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.vidarr.core.ExternalKey;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowRequest {
  private ObjectNode arguments;
  private int attempt;
  private Map<String, Long> consumableResources;
  private ObjectNode engineParameters;
  private Set<ExternalKey> externalKeys;
  private ObjectNode labels;
  private ObjectNode metadata;
  private SubmitMode mode = SubmitMode.RUN;
  private String target;
  private String workflow;
  private String workflowVersion;

  public ObjectNode getArguments() {
    return arguments;
  }

  public int getAttempt() {
    return attempt;
  }

  public Map<String, Long> getConsumableResources() {
    return consumableResources;
  }

  public ObjectNode getEngineParameters() {
    return engineParameters;
  }

  public Set<ExternalKey> getExternalKeys() {
    return externalKeys;
  }

  public ObjectNode getLabels() {
    return labels;
  }

  public ObjectNode getMetadata() {
    return metadata;
  }

  public SubmitMode getMode() {
    return mode;
  }

  public String getTarget() {
    return target;
  }

  public String getWorkflow() {
    return workflow;
  }

  public String getWorkflowVersion() {
    return workflowVersion;
  }

  public void setArguments(ObjectNode arguments) {
    this.arguments = arguments;
  }

  public void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  public void setConsumableResources(Map<String, Long> consumableResources) {
    this.consumableResources = consumableResources;
  }

  public void setEngineParameters(ObjectNode engineParameters) {
    this.engineParameters = engineParameters;
  }

  public void setExternalKeys(Set<ExternalKey> externalKeys) {
    this.externalKeys = externalKeys;
  }

  public void setLabels(ObjectNode labels) {
    this.labels = labels;
  }

  public void setMetadata(ObjectNode metadata) {
    this.metadata = metadata;
  }

  public void setMode(SubmitMode mode) {
    this.mode = mode;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public void setWorkflow(String workflow) {
    this.workflow = workflow;
  }

  public void setWorkflowVersion(String workflowVersion) {
    this.workflowVersion = workflowVersion;
  }
}
