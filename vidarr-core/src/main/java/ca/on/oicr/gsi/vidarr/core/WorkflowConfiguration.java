package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.OutputProvisionType;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import java.util.Map;

/** JSON representation of a workflow definition */
public final class WorkflowConfiguration {
  public static final class Parameter {
    private boolean required = true;
    private InputType type;

    public InputType getType() {
      return type;
    }

    public boolean isRequired() {
      return required;
    }

    public void setRequired(boolean required) {
      this.required = required;
    }

    public void setType(InputType type) {
      this.type = type;
    }
  }

  private String id = "test";
  private WorkflowLanguage language;
  private Map<String, OutputProvisionType> outputs;
  private Map<String, Parameter> parameters;
  private String workflow;

  public String getId() {
    return id;
  }

  public WorkflowLanguage getLanguage() {
    return language;
  }

  public Map<String, OutputProvisionType> getOutputs() {
    return outputs;
  }

  public Map<String, Parameter> getParameters() {
    return parameters;
  }

  public String getWorkflow() {
    return workflow;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setLanguage(WorkflowLanguage language) {
    this.language = language;
  }

  public void setOutputs(Map<String, OutputProvisionType> outputs) {
    this.outputs = outputs;
  }

  public void setParameters(Map<String, Parameter> parameters) {
    this.parameters = parameters;
  }

  public void setWorkflow(String workflow) {
    this.workflow = workflow;
  }

  public WorkflowDefinition toDefinition() {
    return new WorkflowDefinition(
        language,
        id,
        workflow,
        parameters.entrySet().stream()
            .map(
                p ->
                    new WorkflowDefinition.Parameter(
                        p.getValue().type, p.getKey(), p.getValue().required)),
        outputs.entrySet().stream()
            .map(o -> new WorkflowDefinition.Output(o.getValue(), o.getKey())));
  }
}
