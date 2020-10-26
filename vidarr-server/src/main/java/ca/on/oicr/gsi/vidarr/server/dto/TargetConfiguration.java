package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TargetConfiguration {
  private List<String> inputProvisioners;
  private List<String> outputProvisioners;
  private List<String> runtimeProvisioners;
  private String workflowEngine;

  public List<String> getInputProvisioners() {
    return inputProvisioners;
  }

  public List<String> getOutputProvisioners() {
    return outputProvisioners;
  }

  public List<String> getRuntimeProvisioners() {
    return runtimeProvisioners;
  }

  public String getWorkflowEngine() {
    return workflowEngine;
  }

  public void setInputProvisioners(List<String> inputProvisioners) {
    this.inputProvisioners = inputProvisioners;
  }

  public void setOutputProvisioners(List<String> outputProvisioners) {
    this.outputProvisioners = outputProvisioners;
  }

  public void setRuntimeProvisioners(List<String> runtimeProvisioners) {
    this.runtimeProvisioners = runtimeProvisioners;
  }

  public void setWorkflowEngine(String workflowEngine) {
    this.workflowEngine = workflowEngine;
  }
}
