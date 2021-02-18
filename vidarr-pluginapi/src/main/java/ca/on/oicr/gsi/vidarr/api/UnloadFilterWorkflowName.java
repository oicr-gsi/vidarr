package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.UnloadTextSelector;

public final class UnloadFilterWorkflowName implements UnloadFilter {

  private UnloadTextSelector name;

  @Override
  public <T> T convert(Visitor<T> visitor) {
    return visitor.workflowName(name.stream());
  }

  public UnloadTextSelector getName() {
    return name;
  }

  public void setName(UnloadTextSelector name) {
    this.name = name;
  }
}
