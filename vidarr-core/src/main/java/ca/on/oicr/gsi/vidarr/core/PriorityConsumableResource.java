package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import ca.on.oicr.gsi.vidarr.PriorityFormula;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import ca.on.oicr.gsi.vidarr.PriorityScorer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

public final class PriorityConsumableResource implements ConsumableResource {
  private Integer defaultPriority;
  private PriorityFormula formula;
  private Map<String, PriorityInput> inputs;
  private String name;
  private PriorityScorer scorer;

  @Override
  public Optional<HttpHandler> httpHandler() {
    final var routes = scorer.httpHandler().map(Handlers::path).orElseGet(Handlers::path);
    for (final var entry : inputs.entrySet()) {
      entry.getValue().httpHandler().ifPresent(h -> routes.addPrefixPath("/" + entry.getKey(), h));
    }
    return Optional.of(routes);
  }

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    final var type =
        BasicType.object(
            inputs.entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), e.getValue().inputFromSubmitter())));
    return Optional.of(new Pair<>(name, defaultPriority == null ? type : type.asOptional()));
  }

  @Override
  public boolean isInputFromSubmitterRequired() {
    return defaultPriority == null;
  }

  @Override
  public int priority() {
    return -1000;
  }

  @Override
  public void recover(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Optional<JsonNode> resourceJson) {
    scorer.recover(workflowName, workflowVersion, vidarrId);
  }

  @Override
  public void release(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {
    scorer.release(workflowName, workflowVersion, vidarrId);
  }

  @Override
  public ConsumableResourceResponse request(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Instant createdTime,
      OptionalInt workflowMaxInFlight,
      Optional<JsonNode> input) {
    final Optional<Map<String, Integer>> inputValues =
        input.map(
            i ->
                inputs.entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            Map.Entry::getKey,
                            e -> {
                              final var value = i.get(e.getKey());
                              return e.getValue()
                                  .compute(
                                      workflowName,
                                      workflowVersion,
                                      createdTime,
                                      value == null ? NullNode.getInstance() : value);
                            })));
    final int priority =
        inputValues
            .map(
                iv ->
                    formula.compute(name -> iv.getOrDefault(name, Integer.MIN_VALUE), createdTime))
            .orElse(defaultPriority);
    final var available =
        scorer.compute(
            workflowName, workflowVersion, vidarrId, createdTime, workflowMaxInFlight, priority);
    return new ConsumableResourceResponse() {

      @Override
      public <T> T apply(Visitor<T> visitor) {
        inputValues.ifPresentOrElse(
            iv -> iv.forEach((varName, value) -> visitor.set(name + "-" + varName, value)),
            () -> inputs.keySet().forEach(varName -> visitor.clear(name + "-" + varName)));
        visitor.set(name, priority);
        return available ? visitor.available() : visitor.unavailable();
      }
    };
  }

  public void setDefaultPriority(Integer defaultPriority) {
    this.defaultPriority = defaultPriority;
  }

  public void setFormula(PriorityFormula formula) {
    this.formula = formula;
  }

  public void setInputs(Map<String, PriorityInput> inputs) {
    this.inputs = inputs;
  }

  public void setScorer(PriorityScorer scorer) {
    this.scorer = scorer;
  }

  @Override
  public void startup(String name) {
    this.name = name;
    for (final var input : inputs.entrySet()) {
      input.getValue().startup(name, input.getKey());
    }
    scorer.startup();
  }
}
