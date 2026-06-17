package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.OffsetDateTime;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ReprovisionOutRequest {

  private String workflowRunHashId;
  private String outputProvisionerName;
  private String outputPath;
  private Optional<OffsetDateTime> originalCompleted;
  private int attempt;

  public int getAttempt() {
    return attempt;
  }

  public Optional<OffsetDateTime> getOriginalCompleted() {
    return originalCompleted;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public String getWorkflowRunHashId() {
    return workflowRunHashId;
  }

  public String getOutputProvisionerName() {
    return outputProvisionerName;
  }

  public void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  public void setOriginalCompleted(Optional<OffsetDateTime> originalCompleted) {
    this.originalCompleted = originalCompleted;
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

  public void check() throws Exception {
    boolean ok = true;
    StringBuilder error = new StringBuilder("Malformed reprovision-out request: ");
    if (null == workflowRunHashId || workflowRunHashId.isBlank()) {
      error.append("workflowRunHashId is empty. ");
      ok = false;
    }
    if (null == outputPath || outputPath.isBlank()) {
      error.append("outputPath is empty. ");
      ok = false;
    }
    if (null == outputProvisionerName || outputProvisionerName.isBlank()) {
      error.append("outputProvisionerName is empty. ");
      ok = false;
    }
    if (!ok)
      throw new Exception(error.toString());
  }
}