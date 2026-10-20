package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.UnloadTextSelector;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class UnloadFilterWorkflowLabel implements UnloadFilter {

  private final String label;
  private final UnloadTextSelector value;

  @JsonCreator
  public UnloadFilterWorkflowLabel(
      @JsonProperty(value = "label", required = true) String label,
      @JsonProperty(value = "value", required = true) UnloadTextSelector value) {
    this.label = Objects.requireNonNull(label, "label");
    this.value = Objects.requireNonNull(value, "value");
  }

  @Override
  public <T> T convert(Visitor<T> visitor) {
    return visitor.workflowLabel(label, value.stream());
  }

  public String getLabel(){
    return label;
  }

  public UnloadTextSelector getValue(){
    return value;
  }

}
