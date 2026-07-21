package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    this.externalKeys = new ArrayList<>(externalKeys);
    Collections.sort(externalKeys);
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

  /**
   * When importing a workflow run, we don't initially have a workflowRun nor an id for this
   * analysis record.
   * Reprovisioning may change the path of this analysis record, so we only compare the filename,
   * which is not changed by reprovisioning.
   * @return integer hash
   */
  public int hashCode(){
    String fileName = Path.of(path).getFileName().toString();
    return Objects.hash(checksum, checksumType, created, externalKeys.stream().map(ExternalId::new).toList(), fileName, labels, metatype, size, type);
  }

  /**
   * When importing a workflow run, we don't initially have a workflowRun nor an id for this
   * analysis record.
   * Reprovisioning may change the path of this analysis record, so we only compare the filename,
   * which is not changed by reprovisioning.
   * ExternalKeys may be the same but have different versions, which is valid, so cast down to
   * ExternalId to ensure they do not get in the way of equality testing.
   *
   * @param other the reference object with which to compare.
   * @return bool
   */
  public boolean equals(Object other){
    if (this == other) return true;
    if (null == other || getClass() != other.getClass()) return false;

    // K extends ExternalId, this may be a downcast but it is safe
    ProvenanceAnalysisRecord<ExternalId> o = (ProvenanceAnalysisRecord)other;
    boolean ok = this.checksum.equals(o.checksum)
        && this.checksumType.equals(o.checksumType)
        && this.created.equals(o.created)
        && this.labels.equals(o.labels)
        && this.metatype.equals(o.metatype)
        && this.size == o.size
        && this.type.equals(o.type);

    ok &= this.externalKeys.stream()
        .map(ExternalId::new)
        .toList()
        .equals(o.externalKeys.stream()
            .map(ExternalId::new)
            .toList());

    // Path.equals can introduce filesystem-specific differences in equality
    ok &= Path.of(path).getFileName().toString().equals(Path.of(o.path).getFileName().toString());
    return ok;
  }
}
