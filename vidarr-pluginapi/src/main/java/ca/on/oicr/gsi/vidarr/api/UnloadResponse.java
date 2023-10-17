package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UnloadResponse {
  private String filename;
  private List<String> deletedWorkflowRuns;

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public List<String> getDeletedWorkflowRuns() {
    return deletedWorkflowRuns;
  }

  public void setDeletedWorkflowRuns(List<String> deletedWorkflowRuns) {
    this.deletedWorkflowRuns = deletedWorkflowRuns;
  }
}
