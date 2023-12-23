package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class RetryProvisionOutRequest {
  private List<String> workflowRunIds;

  public List<String> getWorkflowRunIds() {
    return workflowRunIds;
  }

  public void setWorkflowRunIds(List<String> workflowRunIds) {
    this.workflowRunIds = workflowRunIds;
  }
}
