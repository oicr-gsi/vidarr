package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.server.WorkflowRouterPredicate;
import java.util.Map;
import javax.xml.stream.XMLStreamException;

public final class RoutingConditionStrEq extends RoutingCondition {
  private String value;
  private String variable;

  public String getValue() {
    return value;
  }

  public String getVariable() {
    return variable;
  }

  @Override
  public WorkflowRouterPredicate resolve(Map<String, RoutingParameterType> routingParameters) {
    final var found = routingParameters.get(variable);
    if (found == null) {
      throw new IllegalArgumentException(String.format("Unknown routing parameter: %s", variable));
    } else if (found != RoutingParameterType.STRING) {
      throw new IllegalArgumentException(
          String.format("Routing parameter %s is %s, but expected STRING", variable, found));
    }
    return new WorkflowRouterPredicate() {
      private final String value = RoutingConditionStrEq.this.value;
      private final String variable = RoutingConditionStrEq.this.variable;

      @Override
      public void display(SectionRenderer sectionRenderer) throws XMLStreamException {
        sectionRenderer.line("String Equals", "");
        sectionRenderer.line("Variable", variable);
        sectionRenderer.line("Value", value);
      }
    };
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void setVariable(String variable) {
    this.variable = variable;
  }
}
