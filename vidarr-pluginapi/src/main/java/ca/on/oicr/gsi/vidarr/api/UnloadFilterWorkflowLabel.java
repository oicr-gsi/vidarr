package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.UnloadTextSelector;

public class UnloadFilterWorkflowLabel implements UnloadFilter {

  private String label;
  private UnloadTextSelector value;

  @Override
  public <T> T convert(Visitor<T> visitor) {
    return visitor.workflowLabel(label, value.stream());
  }

  public String getLabel(){ return label; }

  public void setLabel(String label) {this.label = label;}

  public UnloadTextSelector getValue(){return value;}

  public void setValue(UnloadTextSelector value) {
    this.value = value;
  }
}
