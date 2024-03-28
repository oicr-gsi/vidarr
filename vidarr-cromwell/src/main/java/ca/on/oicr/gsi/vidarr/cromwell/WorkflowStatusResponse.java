package ca.on.oicr.gsi.vidarr.cromwell;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** The response from Cromwell when requesting the status of a workflow */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowStatusResponse {
  private String id;
  private String status;

  public String getId() {
    return id;
  }

  public String getStatus() {
    return status;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public boolean hasSucceeded() {
    return status.equals("Succeeded");
  }
}
