package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.Pair;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ProvenanceWorkflowRun<K extends ExternalId> {

  private List<ProvenanceAnalysisRecord<ExternalId>> analysis;
  private ZonedDateTime completed;
  private ZonedDateTime created;
  private List<K> externalKeys;
  private String id;
  private List<String> inputFiles;
  private String instanceName;
  private Map<String, String> labels;
  private ZonedDateTime modified;
  private ZonedDateTime started;
  private String workflowName;
  private String workflowVersion;

  public List<ProvenanceAnalysisRecord<ExternalId>> getAnalysis() {
    return analysis;
  }

  public ZonedDateTime getCompleted() {
    return completed;
  }

  public ZonedDateTime getCreated() {
    return created;
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

  @JsonIgnore
  public String getInstanceName() {
    return instanceName;
  }

  public Map<String, String> getLabels() {
    return labels;
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

  public void setCompleted(ZonedDateTime completed) {
    this.completed = completed;
  }

  public void setCreated(ZonedDateTime created) {
    this.created = created;
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

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
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
}
