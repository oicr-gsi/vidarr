package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.server.WorkflowRouter;
import java.util.Map;
import javax.xml.stream.XMLStreamException;

public final class RouteDeny extends Route {

  @Override
  public WorkflowRouter resolve(
      Map<String, WorkflowEngine> workflowEngines,
      Map<String, OutputProvisioner> provisioners,
      Map<String, RoutingParameterType> routingParameters,
      WorkflowRouter next) {
    final var predicate = getWhen().resolve(routingParameters);
    return new WorkflowRouter() {
      @Override
      public void display(SectionRenderer sectionRenderer) throws XMLStreamException {
        sectionRenderer.line("Condition", "");
        predicate.display(sectionRenderer);
        sectionRenderer.line("", "Deny");
        next.display(sectionRenderer);
      }
    };
  }
}
