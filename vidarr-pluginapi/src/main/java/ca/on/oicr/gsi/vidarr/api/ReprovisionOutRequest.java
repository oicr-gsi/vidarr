package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ReprovisionOutRequest {

  private String workflowRunHashId;
  private String outputProvisionerName;
  private String outputPath;

  public String getOutputPath() {
    return outputPath;
  }

  public String getWorkflowRunHashId() {
    return workflowRunHashId;
  }

  public String getOutputProvisionerName() {
    return outputProvisionerName;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public void setWorkflowRunHashId(String workflowRunHashId) {
    this.workflowRunHashId = workflowRunHashId;
  }

  public void setOutputProvisionerName(String outputProvisionerName) {
    this.outputProvisionerName = outputProvisionerName;
  }

  public boolean check() {
    return null != workflowRunHashId
        && null != outputProvisionerName
        && null != outputPath
        && !workflowRunHashId.isBlank()
        && !outputProvisionerName.isBlank()
        && !outputPath.isBlank();
  }
}