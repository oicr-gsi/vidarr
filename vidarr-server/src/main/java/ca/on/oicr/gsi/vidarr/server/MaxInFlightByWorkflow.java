package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import ca.on.oicr.gsi.vidarr.api.InFlightCountsByWorkflow;
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
              "The current number of workflows that are running.")
          .labelNames("workflow")
          .register();
  private static final Gauge maxInFlightCount =
      Gauge.build(
              "vidarr_in_flight_per_workflow_max",
              "The maximum number of workflows that can be run simultaneously.")
          .labelNames("workflow")
          .register();
  private final Map<String, MaxState> workflows = new ConcurrentHashMap<>();

  /** Get summary information for each workflow: workflowName -> (currentInFlight, maxInFlight) */
  public InFlightCountsByWorkflow getCountsByWorkflow() {

    InFlightCountsByWorkflow counts = new InFlightCountsByWorkflow();
    for (String name : workflows.keySet()) {
      counts.add(name, workflows.get(name).running.size(), workflows.get(name).maximum);
    }
    return counts;
  }

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return Optional.empty();
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> resourceJson) {
    final var stateRunning = workflows.computeIfAbsent(workflowName, k -> new MaxState()).running;
    // since we just created it if it doesn't exist, no need for null check here
    stateRunning.add(vidarrId);
    currentInFlightCount.labels(workflowName).set(stateRunning.size());
  }

  @Override
  public void release(String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {
    final var state = workflows.get(workflowName);
    if (state != null) {
      state.running.remove(vidarrId);
      currentInFlightCount.labels(workflowName).set(state.running.size());
    }
  }

  @Override
  public synchronized ConsumableResourceResponse request(
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

  @Override
  public void startup(String name) {
    // Always ok.
  }

  public void set(String workflowName, int maxInFlight) {
    maxInFlightCount.labels(workflowName).set(maxInFlight);
    workflows.computeIfAbsent(workflowName, k -> new MaxState()).maximum = maxInFlight;
  }

  @Override
  public boolean isInputFromSubmitterRequired() {
    return false;
  }
}
