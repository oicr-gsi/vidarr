package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.vidarr.server.WorkflowRouterPredicate;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "if")
@JsonSubTypes({
  @JsonSubTypes.Type(value = RoutingConditionStrEq.class, name = "str_eq"),
  @JsonSubTypes.Type(value = RoutingConditionStrIn.class, name = "str_in")
})
public abstract class RoutingCondition {
  RoutingCondition() {}

  public abstract WorkflowRouterPredicate resolve(
      Map<String, RoutingParameterType> routingParameters);
}
