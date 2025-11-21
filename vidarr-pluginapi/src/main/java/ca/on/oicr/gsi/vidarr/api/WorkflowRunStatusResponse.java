package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowRunStatusResponse {

  private ObjectNode arguments;
  private int attempt;
  private ZonedDateTime completed;
  private JsonNode consumableResources;
  private ZonedDateTime created;
  private JsonNode engineParameters;
  private String enginePhase;
  private String id;
  private List<String> inputFiles;
  private Map<String, String> labels;
  private ObjectNode metadata;
  private ZonedDateTime modified;
  private String operationStatus;
  private List<OperationStatusResponse> operations;
  private Boolean preflightOk;
  protected boolean running;
  private ZonedDateTime started;
  private String target;
  private Map<String, Long> tracing;
  private String workflowRunUrl;

  public ObjectNode getArguments() {
    return arguments;
  }

  public int getAttempt() {
    return attempt;
  }

  public ZonedDateTime getCompleted() {
    return completed;
  }

  public JsonNode getConsumableResources() {
    return consumableResources;
  }

  public ZonedDateTime getCreated() {
    return created;
  }

  public JsonNode getEngineParameters() {
    return engineParameters;
  }

  public String getEnginePhase() {
    return enginePhase;
  }

  public String getId() {
    return id;
  }

  public List<String> getInputFiles() {
    return inputFiles;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public ObjectNode getMetadata() {
    return metadata;
  }

  public ZonedDateTime getModified() {
    return modified;
  }

  public String getOperationStatus() {
    return operationStatus;
  }

  public List<OperationStatusResponse> getOperations() {
    return operations;
  }

  public Boolean getPreflightOk() {
    return preflightOk;
  }

  public ZonedDateTime getStarted() {
    return started;
  }

  public String getTarget() {
    return target;
  }

  public Map<String, Long> getTracing() {
    return tracing;
  }

  public String getWorkflowRunUrl() {
    return workflowRunUrl;
  }

  public boolean isRunning() {
    return running;
  }

  public void setArguments(ObjectNode arguments) {
    this.arguments = arguments;
  }

  public void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  public void setCompleted(ZonedDateTime completed) {
    this.completed = completed;
  }

  public void setConsumableResources(JsonNode consumableResources) {
    this.consumableResources = consumableResources;
  }

  public void setCreated(ZonedDateTime created) {
    this.created = created;
  }

  public void setEngineParameters(JsonNode engineParameters) {
    this.engineParameters = engineParameters;
  }

  public void setEnginePhase(String enginePhase) {
    this.enginePhase = enginePhase;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setInputFiles(List<String> inputFiles) {
    this.inputFiles = inputFiles;
  }

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }

  public void setMetadata(ObjectNode metadata) {
    this.metadata = metadata;
  }

  public void setModified(ZonedDateTime modified) {
    this.modified = modified;
  }

  public void setOperationStatus(String operationStatus) {
    this.operationStatus = operationStatus;
  }

  public void setOperations(List<OperationStatusResponse> operations) {
    this.operations = operations;
  }

  public void setPreflightOk(Boolean preflightOk) {
    this.preflightOk = preflightOk;
  }

  public void setRunning(boolean running) {
    this.running = running;
  }

  public void setStarted(ZonedDateTime started) {
    this.started = started;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public void setTracing(Map<String, Long> tracing) {
    this.tracing = tracing;
  }

  public void setWorkflowRunUrl(String workflowRunUrl) {
    this.workflowRunUrl = workflowRunUrl;
  }
}
