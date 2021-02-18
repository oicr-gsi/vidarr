package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.UnloadTextSelector;

public final class UnloadFilterWorkflowId implements UnloadFilter {

  private UnloadTextSelector id;

  @Override
  public <T> T convert(Visitor<T> visitor) {
    return visitor.workflowId(id.stream());
  }

  public UnloadTextSelector getId() {
    return id;
  }

  public void setId(UnloadTextSelector id) {
    this.id = id;
  }
}
