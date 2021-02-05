package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import java.util.Map;

public final class WorkflowDeclaration {
  private Map<String, BasicType> labels;
  private WorkflowLanguage language;
  private Map<String, OutputType> metadata;
  private String name;
  private Map<String, InputType> parameters;
  private String version;

  public Map<String, BasicType> getLabels() {
    return labels;
  }

  public WorkflowLanguage getLanguage() {
    return language;
  }

  public Map<String, OutputType> getMetadata() {
    return metadata;
  }

  public String getName() {
    return name;
  }

  public Map<String, InputType> getParameters() {
    return parameters;
  }

  public String getVersion() {
    return version;
  }

  public void setLabels(Map<String, BasicType> labels) {
    this.labels = labels;
  }

  public void setLanguage(WorkflowLanguage language) {
    this.language = language;
  }

  public void setMetadata(Map<String, OutputType> metadata) {
    this.metadata = metadata;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setParameters(Map<String, InputType> parameters) {
    this.parameters = parameters;
  }

  public void setVersion(String version) {
    this.version = version;
  }
}
