package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UnloadedWorkflowVersion extends BaseWorkflowConfiguration {

  private String name;
  private String version;

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o){
    if (this == o) return true;
    if (null == o || this.getClass() != o.getClass()) return false;
    UnloadedWorkflowVersion other = (UnloadedWorkflowVersion) o;

    return super.equals(other)
        && Objects.equals(this.name, other.name)
        && Objects.equals(this.version, other.version);
  }

  @Override
  public int hashCode(){
    return Objects.hash(super.hashCode(), name, version);
  }


}
