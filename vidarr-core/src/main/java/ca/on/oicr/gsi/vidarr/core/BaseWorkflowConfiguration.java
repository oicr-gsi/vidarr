package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.OutputProvisionType;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import java.util.Map;

/** JSON representation of a workflow definition */
public abstract class BaseWorkflowConfiguration {
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

  private WorkflowLanguage language;
  private Map<String, OutputProvisionType> outputs;
  private Map<String, Parameter> parameters;
  private String workflow;

  public final WorkflowLanguage getLanguage() {
    return language;
  }

  public final Map<String, OutputProvisionType> getOutputs() {
    return outputs;
  }

  public final Map<String, Parameter> getParameters() {
    return parameters;
  }

  public final String getWorkflow() {
    return workflow;
  }

  public final void setLanguage(WorkflowLanguage language) {
    this.language = language;
  }

  public final void setOutputs(Map<String, OutputProvisionType> outputs) {
    this.outputs = outputs;
  }

  public final void setParameters(Map<String, Parameter> parameters) {
    this.parameters = parameters;
  }

  public final void setWorkflow(String workflow) {
    this.workflow = workflow;
  }
}
