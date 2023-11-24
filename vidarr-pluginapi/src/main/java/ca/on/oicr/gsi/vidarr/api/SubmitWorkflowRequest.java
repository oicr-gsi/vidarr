package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowRequest {
  private ObjectNode arguments;
  private int attempt;
  private Map<String, JsonNode> consumableResources;
  private JsonNode engineParameters;
  private Set<ExternalKey> externalKeys;
  private ObjectNode labels;
  private ObjectNode metadata;
  private SubmitMode mode = SubmitMode.RUN;
  private String target;
  private String workflow;
  private String workflowVersion;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SubmitWorkflowRequest that = (SubmitWorkflowRequest) o;
    return attempt == that.attempt && Objects.equals(consumableResources, that.consumableResources) && equalsIgnoreAttemptAndConsumableResources(that);
  }

  public boolean equalsIgnoreAttemptAndConsumableResources(SubmitWorkflowRequest that) {
    return Objects.equals(arguments, that.arguments)
        && Objects.equals(engineParameters, that.engineParameters)
        && Objects.equals(externalKeys, that.externalKeys)
        && Objects.equals(labels, that.labels)
        && Objects.equals(metadata, that.metadata)
        && mode == that.mode
        && Objects.equals(target, that.target)
        && Objects.equals(workflow, that.workflow)
        && Objects.equals(workflowVersion, that.workflowVersion);
  }

  public ObjectNode getArguments() {
    return arguments;
  }

  public int getAttempt() {
    return attempt;
  }

  public Map<String, JsonNode> getConsumableResources() {
    return consumableResources;
  }

  public JsonNode getEngineParameters() {
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

  @Override
  public int hashCode() {
    return hashCodeIgnoreAttemptAndConsumableResources() * 31 + Integer.hashCode(attempt) + Objects.hashCode(consumableResources);
  }

  public int hashCodeIgnoreAttemptAndConsumableResources() {
    return Objects.hash(
            arguments,
            engineParameters,
            externalKeys,
            labels,
            metadata,
            mode,
            target,
            workflow,
            workflowVersion);
  }

  public void setArguments(ObjectNode arguments) {
    this.arguments = arguments;
  }

  public void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  public void setConsumableResources(Map<String, JsonNode> consumableResources) {
    this.consumableResources = consumableResources;
  }

  public void setEngineParameters(JsonNode engineParameters) {
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
