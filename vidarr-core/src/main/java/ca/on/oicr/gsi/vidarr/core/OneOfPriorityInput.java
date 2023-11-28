package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public final class OneOfPriorityInput implements PriorityInput {
  private int defaultPriority;
  private Map<String, PriorityInput> inputs;

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    if (!input.has("type")) {
      return defaultPriority;
    }
    final var type = input.get("type");
    if (!type.isTextual()) {
      return defaultPriority;
    }
    final var priorityInput = inputs.get(type.asText());
    if (priorityInput == null) {
      return defaultPriority;
    }
    final var contents = input.get("contents");
    return priorityInput.compute(
        workflowName,
        workflowVersion,
        created,
        contents == null ? NullNode.getInstance() : contents);
  }

  public int getDefaultPriority() {
    return defaultPriority;
  }

  public Map<String, PriorityInput> getInputs() {
    return inputs;
  }

  @Override
  public Optional<HttpHandler> httpHandler() {
    final var routes = Handlers.path();
    for (final var input : inputs.entrySet()) {
      input
          .getValue()
          .httpHandler()
          .ifPresent(handler -> routes.addPrefixPath(input.getKey(), handler));
    }
    return Optional.of(routes);
  }

  @Override
  public BasicType inputFromSubmitter() {
    return BasicType.taggedUnionFromPairs(
        inputs.entrySet().stream()
            .map(e -> new Pair<>(e.getKey(), e.getValue().inputFromSubmitter())));
  }

  public void setDefaultPriority(int defaultPriority) {
    this.defaultPriority = defaultPriority;
  }

  public void setInputs(Map<String, PriorityInput> inputs) {
    this.inputs = inputs;
  }

  @Override
  public void startup(String resourceName, String inputName) {
    for (final var input : inputs.entrySet()) {
      input.getValue().startup(resourceName, inputName + " " + input.getKey());
    }
  }
}
