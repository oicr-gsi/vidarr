package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.BasicType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.Objects;

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

  @Override
  public boolean equals(Object o){
    if (this == o) return true;
    if (null == o || this.getClass() != o.getClass()) return false;
    UnloadedWorkflow other = (UnloadedWorkflow) o;
    return Objects.equals(this.labels, other.labels)
        && Objects.equals(this.name, other.name);
  }

  @Override
  public int hashCode(){
    return Objects.hash(labels, name);
  }
}
