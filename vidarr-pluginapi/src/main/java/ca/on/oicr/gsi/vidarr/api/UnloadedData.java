package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UnloadedData {

  private List<ProvenanceWorkflowRun<ExternalKey>> workflowRuns;
  private List<UnloadedWorkflowVersion> workflowVersions;
  private List<UnloadedWorkflow> workflows;

  public List<ProvenanceWorkflowRun<ExternalKey>> getWorkflowRuns() {
    return workflowRuns;
  }

  public List<UnloadedWorkflowVersion> getWorkflowVersions() {
    return workflowVersions;
  }

  public List<UnloadedWorkflow> getWorkflows() {
    return workflows;
  }

  public void setWorkflowRuns(List<ProvenanceWorkflowRun<ExternalKey>> workflowRuns) {
    this.workflowRuns = workflowRuns;
  }

  public void setWorkflowVersions(List<UnloadedWorkflowVersion> workflowVersions) {
    this.workflowVersions = workflowVersions;
  }

  public void setWorkflows(List<UnloadedWorkflow> workflows) {
    this.workflows = workflows;
  }
}
