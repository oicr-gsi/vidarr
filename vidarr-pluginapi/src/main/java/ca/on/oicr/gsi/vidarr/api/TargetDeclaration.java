package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class TargetDeclaration {
  private Map<String, BasicType> consumableResources = Collections.emptyMap();
  private BasicType engineParameters;
  private Map<InputProvisionFormat, BasicType> inputProvisioners;
  private List<WorkflowLanguage> language;
  private Map<OutputProvisionFormat, BasicType> outputProvisioners;

  public Map<String, BasicType> getConsumableResources() {
    return consumableResources;
  }

  public BasicType getEngineParameters() {
    return engineParameters;
  }

  public Map<InputProvisionFormat, BasicType> getInputProvisioners() {
    return inputProvisioners;
  }

  public List<WorkflowLanguage> getLanguage() {
    return language;
  }

  public Map<OutputProvisionFormat, BasicType> getOutputProvisioners() {
    return outputProvisioners;
  }

  public void setConsumableResources(
      Map<String, BasicType> consumableResources) {
    this.consumableResources = consumableResources;
  }

  public void setEngineParameters(BasicType engineParameters) {
    this.engineParameters = engineParameters;
  }

  public void setInputProvisioners(Map<InputProvisionFormat, BasicType> inputProvisioners) {
    this.inputProvisioners = inputProvisioners;
  }

  public void setLanguage(List<WorkflowLanguage> language) {
    this.language = language;
  }

  public void setOutputProvisioners(Map<OutputProvisionFormat, BasicType> outputProvisioners) {
    this.outputProvisioners = outputProvisioners;
  }
}
