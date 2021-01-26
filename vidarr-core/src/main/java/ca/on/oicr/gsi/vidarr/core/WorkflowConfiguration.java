package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.WorkflowDefinition;

/** JSON representation of a workflow definition */
public class WorkflowConfiguration extends BaseWorkflowConfiguration {

  private String id = "test";

  public String getId() {
    return id;
  }

  public WorkflowDefinition toDefinition() {
    return new WorkflowDefinition(
        getLanguage(),
        id,
        getWorkflow(),
        getParameters().entrySet().stream()
            .map(p -> new WorkflowDefinition.Parameter(p.getValue(), p.getKey())),
        getOutputs().entrySet().stream()
            .map(o -> new WorkflowDefinition.Output(o.getValue(), o.getKey())));
  }
}
