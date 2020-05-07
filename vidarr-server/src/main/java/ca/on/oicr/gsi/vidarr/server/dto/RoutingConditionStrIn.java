package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.server.WorkflowRouterPredicate;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLStreamException;

public final class RoutingConditionStrIn extends RoutingCondition {

  private List<String> values;
  private String variable;

  public List<String> getValues() {
    return values;
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
      private final List<String> values = RoutingConditionStrIn.this.values;
      private final String variable = RoutingConditionStrIn.this.variable;

      @Override
      public void display(SectionRenderer sectionRenderer) throws XMLStreamException {
        sectionRenderer.line("String In Set", "");
        sectionRenderer.line("Variable", variable);
        for (final var value : values) {
          sectionRenderer.line("Value", value);
        }
      }
    };
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  public void setVariable(String variable) {
    this.variable = variable;
  }
}
