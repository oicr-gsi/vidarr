package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.databind.JsonNode;
import io.prometheus.client.Gauge;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class MaxInFlightByWorkflow implements ConsumableResource {
  private static final class MaxState {
    private int maximum;
    private final Set<String> running = ConcurrentHashMap.newKeySet();
  }

  private static final Gauge currentInFlightCount =
      Gauge.build(
              "vidarr_in_flight_per_workflow_current",
              "The current number of workflows are running.")
          .labelNames("workflow")
          .register();
  private static final Gauge maxInFlightCount =
      Gauge.build(
              "vidarr_in_flight_per_workflow_max",
              "The maximum number of workflows that can be run simultaneously.")
          .labelNames("workflow")
          .register();
  private final Map<String, MaxState> workflows = new ConcurrentHashMap<>();

  @Override
  public Optional<Pair<String, BasicType>> inputFromUser() {
    return Optional.empty();
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId) {
    workflows.computeIfAbsent(workflowName, k -> new MaxState()).running.add(vidarrId);
  }

  @Override
  public void release(String workflowName, String workflowVersion, String vidarrId) {
    final var state = workflows.get(workflowName);
    if (state != null) {
      state.running.remove(vidarrId);
      currentInFlightCount.labels(workflowName).set(state.running.size());
    }
  }

  @Override
  public ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {
    final var state = workflows.get(workflowName);
    if (state == null) {
      return ConsumableResourceResponse.error(
          "Internal Vidarr error: max in flight has not been configured despite being in the"
              + " database.");
    } else {
      if (state.maximum > state.running.size()) {
        state.running.add(vidarrId);
        currentInFlightCount.labels(workflowName).set(state.running.size());
        return ConsumableResourceResponse.AVAILABLE;
      } else {
        return ConsumableResourceResponse.error(
            String.format("The maximum number of %s workflows has been reached.", workflowName));
      }
    }
  }

  public void set(String workflowName, int maxInFlight) {
    maxInFlightCount.labels(workflowName).set(maxInFlight);
    workflows.computeIfAbsent(workflowName, k -> new MaxState()).maximum = maxInFlight;
  }
}