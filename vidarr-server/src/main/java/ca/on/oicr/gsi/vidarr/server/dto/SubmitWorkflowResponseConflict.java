package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowResponseConflict extends SubmitWorkflowResponse {

  public SubmitWorkflowResponseConflict() {}

  public SubmitWorkflowResponseConflict(List<String> ids) {
    this.ids = ids;
  }

  private List<String> ids;

  public List<String> getIds() {
    return ids;
  }

  public void setIds(List<String> ids) {
    this.ids = ids;
  }
}
