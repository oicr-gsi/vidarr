package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowRequest {
  private Map<String, Long> consumableResources;
  private ObjectNode inputs;
  private Map<String, String> labels;
  private SubmitMode mode = SubmitMode.RUN;
  private Map<String, OutputAssociation> outputs;
  private ObjectNode routing;
  private String workflow;

  public Map<String, Long> getConsumableResources() {
    return consumableResources;
  }

  public ObjectNode getInputs() {
    return inputs;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public SubmitMode getMode() {
    return mode;
  }

  public Map<String, OutputAssociation> getOutputs() {
    return outputs;
  }

  public ObjectNode getRouting() {
    return routing;
  }

  public String getWorkflow() {
    return workflow;
  }

  public void setConsumableResources(Map<String, Long> consumableResources) {
    this.consumableResources = consumableResources;
  }

  public void setInputs(ObjectNode inputs) {
    this.inputs = inputs;
  }

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }

  public void setMode(SubmitMode mode) {
    this.mode = mode;
  }

  public void setOutputs(Map<String, OutputAssociation> outputs) {
    this.outputs = outputs;
  }

  public void setRouting(ObjectNode routing) {
    this.routing = routing;
  }

  public void setWorkflow(String workflow) {
    this.workflow = workflow;
  }
}
