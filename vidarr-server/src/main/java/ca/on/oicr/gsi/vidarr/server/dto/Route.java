package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.server.WorkflowRouter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "action")
@JsonSubTypes({
  @JsonSubTypes.Type(value = RouteAccept.class, name = "accept"),
  @JsonSubTypes.Type(value = RouteDeny.class, name = "deny")
})
public abstract class Route {
  private RoutingCondition when;

  Route() {}

  public RoutingCondition getWhen() {
    return when;
  }

  public abstract WorkflowRouter resolve(
      Map<String, WorkflowEngine> workflowEngines,
      Map<String, OutputProvisioner> provisioners,
      Map<String, RoutingParameterType> routingParameters,
      WorkflowRouter next);

  public void setWhen(RoutingCondition when) {
    this.when = when;
  }
}
