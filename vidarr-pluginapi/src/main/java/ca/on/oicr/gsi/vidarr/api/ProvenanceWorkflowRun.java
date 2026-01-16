package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.Pair;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ProvenanceWorkflowRun<K extends ExternalId> {
  private List<ProvenanceAnalysisRecord<ExternalId>> analysis;
  private ObjectNode arguments;
  private ZonedDateTime completed;
  private ZonedDateTime created;
  private JsonNode engineParameters;
  private List<K> externalKeys;
  private String id;
  private List<String> inputFiles;
  private String instanceName;
  private ObjectNode labels;
  private ZonedDateTime lastAccessed;
  private ObjectNode metadata;
  private ZonedDateTime modified;
  private ZonedDateTime started;
  private String workflowName;
  private String workflowVersion;

  public List<ProvenanceAnalysisRecord<ExternalId>> getAnalysis() {
    return analysis;
  }

  public ObjectNode getArguments() {
    return arguments;
  }

  public ZonedDateTime getCompleted() {
    return completed;
  }

  public ZonedDateTime getCreated() {
    return created;
  }

  public JsonNode getEngineParameters() {
    return engineParameters;
  }

  public List<K> getExternalKeys() {
    return externalKeys;
  }

  public String getId() {
    return id;
  }

  public List<String> getInputFiles() {
    return inputFiles;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public ObjectNode getLabels() {
    return labels;
  }

  public ZonedDateTime getLastAccessed() {
    return lastAccessed;
  }

  public ObjectNode getMetadata() {
    return metadata;
  }

  public ZonedDateTime getModified() {
    return modified;
  }

  public ZonedDateTime getStarted() {
    return started;
  }

  public String getWorkflowName() {
    return workflowName;
  }

  public String getWorkflowVersion() {
    return workflowVersion;
  }

  public Stream<Pair<String, String>> key() {
    return externalKeys.stream().map(k -> new Pair<>(k.getProvider(), k.getId()));
  }

  public void setAnalysis(List<ProvenanceAnalysisRecord<ExternalId>> analysis) {
    this.analysis = analysis;
  }

  public void setArguments(ObjectNode arguments) {
    this.arguments = arguments;
  }

  public void setCompleted(ZonedDateTime completed) {
    this.completed = completed;
  }

  public void setCreated(ZonedDateTime created) {
    this.created = created;
  }

  public void setEngineParameters(JsonNode engineParameters) {
    this.engineParameters = engineParameters;
  }

  public void setExternalKeys(List<K> externalKeys) {
    this.externalKeys = externalKeys;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setInputFiles(List<String> inputFiles) {
    this.inputFiles = inputFiles;
  }

  public void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  public void setLabels(ObjectNode labels) {
    this.labels = labels;
  }

  public void setLastAccessed(ZonedDateTime lastAccessed) {
    this.lastAccessed = lastAccessed;
  }

  public void setMetadata(ObjectNode metadata) {
    this.metadata = metadata;
  }

  public void setModified(ZonedDateTime modified) {
    this.modified = modified;
  }

  public void setStarted(ZonedDateTime started) {
    this.started = started;
  }

  public void setWorkflowName(String workflowName) {
    this.workflowName = workflowName;
  }

  public void setWorkflowVersion(String workflowVersion) {
    this.workflowVersion = workflowVersion;
  }

  @Override
  public boolean equals(Object o){
    if (this == o) return true;
    if (null == o || getClass() != o.getClass()) return false;
    ProvenanceWorkflowRun<K> other = (ProvenanceWorkflowRun<K>) o;

    return Objects.deepEquals(this.analysis, other.analysis)
        && Objects.equals(this.arguments, other.arguments)
        && Objects.equals(this.completed, other.completed)
        && Objects.equals(this.created, other.created)
        && Objects.equals(this.engineParameters, other.engineParameters)
        && Objects.deepEquals(this.externalKeys, other.externalKeys)
        && Objects.equals(this.id, other.id)
        && Objects.deepEquals(this.inputFiles, other.inputFiles)
        && Objects.equals(this.instanceName, other.instanceName)
        && Objects.equals(this.labels, other.labels)
        && Objects.equals(this.metadata, other.metadata)
        && Objects.equals(this.started, other.started)
        && Objects.equals(this.workflowName, other.workflowName)
        && Objects.equals(this.workflowVersion, other.workflowVersion);
  }

  @Override
  public int hashCode(){
    return Objects.hash(analysis, arguments, completed, created, engineParameters, externalKeys,
        id, inputFiles, instanceName, labels, metadata, started,
        workflowName, workflowVersion);
  }
}
