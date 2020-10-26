package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.vidarr.core.ExternalKey;
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

  public void setId(String id) {
    this.id = id;
  }
}
