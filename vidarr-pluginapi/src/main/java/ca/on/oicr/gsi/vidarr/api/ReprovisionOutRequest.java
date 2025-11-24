package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ReprovisionOutRequest {

  private List<String> workflowRunHashIds;
  private String outputProvisionerName;
  private String outputPath;

  public String getOutputPath() {
    return outputPath;
  }

  public List<String> getWorkflowRunHashIds() {
    return workflowRunHashIds;
  }

  public String getOutputProvisionerName() {
    return outputProvisionerName;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public void setWorkflowRunHashIds(List<String> workflowRunHashIds) {
    this.workflowRunHashIds = workflowRunHashIds;
  }

  public void setOutputProvisionerName(String outputProvisionerName) {
    this.outputProvisionerName = outputProvisionerName;
  }

  // TODO looks like the handler has an Invalid Request method for itself?
  public boolean check() {
    return null != workflowRunHashIds
        && null != outputProvisionerName
        && null != outputPath
        && !workflowRunHashIds.isEmpty()
        && !outputProvisionerName.isBlank()
        && !outputPath.isBlank();
  }
}