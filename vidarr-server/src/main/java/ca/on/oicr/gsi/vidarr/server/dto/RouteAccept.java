package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.server.WorkflowRouter;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.xml.stream.XMLStreamException;

public final class RouteAccept extends Route {

  private List<String> provisioners;
  private String workflowEngine;

  public List<String> getProvisioners() {
    return provisioners;
  }

  public String getWorkflowEngine() {
    return workflowEngine;
  }

  @Override
  public WorkflowRouter resolve(
      Map<String, WorkflowEngine> workflowEngines,
      Map<String, OutputProvisioner> provisioners,
      Map<String, RoutingParameterType> routingParameters,
      WorkflowRouter next) {
    final var engine =
        Objects.requireNonNull(
            workflowEngines.get(workflowEngine),
            String.format("The workflow engine %s is not configured.", workflowEngine));
    final var provisionMap =
        new EnumMap<OutputProvisionFormat, OutputProvisioner>(OutputProvisionFormat.class);
    for (final var provisionerName : this.provisioners) {
      final var provisioner =
          Objects.requireNonNull(
              provisioners.get(provisionerName),
              String.format("The provisioner %s is not configured.", provisionerName));
      for (final var type : OutputProvisionFormat.values()) {
        if (provisioner.canProvision(type)) {
          if (provisionMap.containsKey(type)) {
            throw new IllegalArgumentException(
                String.format(
                    "The provisioner %s handles %s, but this is already covered by another"
                        + " provisioner.",
                    provisionerName, type));
          } else {
            provisionMap.put(type, provisioner);
          }
        }
      }
    }
    final var predicate = getWhen().resolve(routingParameters);

    return new WorkflowRouter() {
      private final String engineName = RouteAccept.this.workflowEngine;

      @Override
      public void display(SectionRenderer sectionRenderer) throws XMLStreamException {
        sectionRenderer.line("Condition", "");
        predicate.display(sectionRenderer);
        sectionRenderer.line("Workflow", engineName);
        for (final var provisioner : provisionMap.entrySet()) {
          sectionRenderer.line(
              "Provision " + provisioner.getKey().name(), provisioner.getValue().type());
        }
        sectionRenderer.line("", "Next");
        next.display(sectionRenderer);
      }
    };
  }

  public void setProvisioners(List<String> provisioners) {
    this.provisioners = provisioners;
  }

  public void setWorkflowEngine(String workflowEngine) {
    this.workflowEngine = workflowEngine;
  }
}
