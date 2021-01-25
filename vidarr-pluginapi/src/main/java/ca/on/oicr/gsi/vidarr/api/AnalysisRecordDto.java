package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AnalysisRecordDto {
  private ZonedDateTime created;
  private List<ExternalKey> externalKeys;
  private String filePath;
  private long fileSize;
  private String id;
  private Map<String, String> labels;
  private String md5sum;
  private String metatype;
  private ZonedDateTime modified;
  private String type;
  private String workflowRun;

  public ZonedDateTime getCreated() {
    return created;
  }

  public List<ExternalKey> getExternalKeys() {
    return externalKeys;
  }

  public String getFilePath() {
    return filePath;
  }

  public long getFileSize() {
    return fileSize;
  }

  public String getId() {
    return id;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public String getMd5sum() {
    return md5sum;
  }

  public String getMetatype() {
    return metatype;
  }

  public ZonedDateTime getModified() {
    return modified;
  }

  public String getType() {
    return type;
  }

  public String getWorkflowRun() {
    return workflowRun;
  }

  public void setCreated(ZonedDateTime created) {
    this.created = created;
  }

  public void setExternalKeys(List<ExternalKey> externalKeys) {
    this.externalKeys = externalKeys;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }

  public void setMd5sum(String md5sum) {
    this.md5sum = md5sum;
  }

  public void setMetatype(String metatype) {
    this.metatype = metatype;
  }

  public void setModified(ZonedDateTime modified) {
    this.modified = modified;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setWorkflowRun(String workflowRun) {
    this.workflowRun = workflowRun;
  }
}
