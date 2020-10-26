package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowResponseSuccess extends SubmitWorkflowResponse {
  public SubmitWorkflowResponseSuccess() {}

  public SubmitWorkflowResponseSuccess(String id) {
    this.id = id;
  }

  private String id;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
