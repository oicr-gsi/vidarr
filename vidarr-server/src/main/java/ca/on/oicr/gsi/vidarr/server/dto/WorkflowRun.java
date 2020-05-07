package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.ZonedDateTime;
import java.util.List;

public final class WorkflowRun {
  private ZonedDateTime created;
  private ObjectNode input;
  private ZonedDateTime modified;
  private List<String> provisionEngines;
  private List<ProvisionedResult> results;
  private WorkflowStatus status;
  private String url;
  private String workflow;
  private String workflowEngine;

  public ZonedDateTime getCreated() {
    return created;
  }

  public ObjectNode getInput() {
    return input;
  }

  public ZonedDateTime getModified() {
    return modified;
  }

  public List<String> getProvisionEngines() {
    return provisionEngines;
  }

  public List<ProvisionedResult> getResults() {
    return results;
  }

  public String getUrl() {
    return url;
  }

  public String getWorkflow() {
    return workflow;
  }

  public String getWorkflowEngine() {
    return workflowEngine;
  }

  public void setCreated(ZonedDateTime created) {
    this.created = created;
  }

  public void setInput(ObjectNode input) {
    this.input = input;
  }

  public void setModified(ZonedDateTime modified) {
    this.modified = modified;
  }

  public void setProvisionEngines(List<String> provisionEngines) {
    this.provisionEngines = provisionEngines;
  }

  public void setResults(List<ProvisionedResult> results) {
    this.results = results;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setWorkflow(String workflow) {
    this.workflow = workflow;
  }

  public void setWorkflowEngine(String workflowEngine) {
    this.workflowEngine = workflowEngine;
  }
}
