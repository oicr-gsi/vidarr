package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProvenanceAnalysisRecord<K extends ExternalId> {
  private ZonedDateTime created;
  private List<K> externalKeys;
  private String id;
  private Map<String, String> labels;
  private String checksum;
  private String checksumType;
  private String metatype;
  private ZonedDateTime modified;
  private String path;
  private long size;
  private String type;
  private String url;
  private String workflowRun;

  public String getChecksumType() {
    return checksumType;
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

  public Map<String, String> getLabels() {
    return labels;
  }

  public String getChecksum() {
    return checksum;
  }

  public String getMetatype() {
    return metatype;
  }

  public ZonedDateTime getModified() {
    return modified;
  }

  public String getPath() {
    return path;
  }

  public long getSize() {
    return size;
  }

  public String getType() {
    return type;
  }

  public String getUrl() {
    return url;
  }

  public String getWorkflowRun() {
    return workflowRun;
  }

  public void setChecksumType(String checksumType) {
    this.checksumType = checksumType;
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

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }

  public void setChecksum(String checksum) {
    this.checksum = checksum;
  }

  public void setMetatype(String metatype) {
    this.metatype = metatype;
  }

  public void setModified(ZonedDateTime modified) {
    this.modified = modified;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setWorkflowRun(String workflowRun) {
    this.workflowRun = workflowRun;
  }
}
