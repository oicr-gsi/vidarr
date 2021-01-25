package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowResponseMissingKeyVersions extends SubmitWorkflowResponse {
  private String id;
  private List<ExternalKey> keys;

  public SubmitWorkflowResponseMissingKeyVersions() {}

  public SubmitWorkflowResponseMissingKeyVersions(String id, List<ExternalKey> keys) {
    this.id = id;
    this.keys = keys;
  }

  public String getId() {
    return id;
  }

  public List<ExternalKey> getKeys() {
    return keys;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setKeys(List<ExternalKey> keys) {
    this.keys = keys;
  }
}
