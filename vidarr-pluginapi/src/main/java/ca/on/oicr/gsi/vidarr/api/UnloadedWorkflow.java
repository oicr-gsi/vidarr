package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.BasicType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class UnloadedWorkflow {

  private Map<String, BasicType> labels;
  private String name;

  public Map<String, BasicType> getLabels() {
    return labels;
  }

  public String getName() {
    return name;
  }

  public void setLabels(Map<String, BasicType> labels) {
    this.labels = labels;
  }

  public void setName(String name) {
    this.name = name;
  }
}
