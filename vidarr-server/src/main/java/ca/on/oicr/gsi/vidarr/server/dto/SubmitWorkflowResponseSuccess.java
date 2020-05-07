package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.ZonedDateTime;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowResponseSuccess extends SubmitWorkflowResponse {
  private ZonedDateTime created;
  // TODO: debug info (JSON – for display in Shesmu; log location, Cromwell ID, input parameters,
  // Cromwell instance URL)
  // TODO: potential matches – similar to current data in Shesmu? If everything gets skipped, this
  // might be useless
  private List<ProvisionedResult> results;
  private WorkflowStatus status;
  private ZonedDateTime updated;
  private String url;

  public ZonedDateTime getCreated() {
    return created;
  }

  public List<ProvisionedResult> getResults() {
    return results;
  }

  public WorkflowStatus getStatus() {
    return status;
  }

  public ZonedDateTime getUpdated() {
    return updated;
  }

  public String getUrl() {
    return url;
  }

  public void setCreated(ZonedDateTime created) {
    this.created = created;
  }

  public void setResults(List<ProvisionedResult> results) {
    this.results = results;
  }

  public void setStatus(WorkflowStatus status) {
    this.status = status;
  }

  public void setUpdated(ZonedDateTime updated) {
    this.updated = updated;
  }

  public void setUrl(String url) {
    this.url = url;
  }
}
