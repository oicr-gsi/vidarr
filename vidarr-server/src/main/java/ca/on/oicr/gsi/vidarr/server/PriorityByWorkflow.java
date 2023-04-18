package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.databind.JsonNode;
import io.prometheus.client.Gauge;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;


final class PriorityByWorkflow implements ConsumableResource {

  private Integer priority;

  private static final class WaitingState {

    private final SortedSet<SimpleEntry<String, Integer>> waiting = new TreeSet(
        new PairComparator());
  }

  static class PairComparator implements Comparator<SimpleEntry<String, Integer>> {

    @Override
    public int compare(SimpleEntry<String, Integer> o1, SimpleEntry<String, Integer> o2) {
      return o1.getValue() - o2.getValue();
    }
  }

  private static final Gauge currentInWaitingCount =
      Gauge.build(
              "vidarr_in_waiting_per_workflow_current",
              "The current number of workflows are waiting to run.")
          .labelNames("workflow")
          .register();

  private final Map<String, WaitingState> workflows = new ConcurrentHashMap<>();

  /**
   * Get summary information for each workflow: workflowName -> (currentInFlight, maxInFlight)
   */

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return Optional.empty();
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId) {
    final var stateWaiting = workflows.computeIfAbsent(workflowName, k -> new WaitingState()).waiting;
    // since we just created it if it doesn't exist, no need for null check here
    stateWaiting.add(new SimpleEntry(vidarrId, this.priority));
    currentInWaitingCount.labels(workflowName).set(stateWaiting.size());
  }

  @Override
  public void release(String workflowName, String workflowVersion, String vidarrId) {
    final var state = workflows.get(workflowName);
    if (state != null) {
      state.waiting.remove(vidarrId);
      currentInWaitingCount.labels(workflowName).set(state.waiting.size());
    }
  }

  @Override
  public synchronized ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {
    if (this.priority == null) {
      this.priority = 4;
    } else if (!Arrays.asList(-1, 1, 2, 3, 4).contains(this.priority)){
      return ConsumableResourceResponse.error(
          "Internal Vidarr error: the workflow run priority must be a value between 1 and 4.");
    }
    final var state = workflows.get(workflowName);
    if (state == null) {
      return ConsumableResourceResponse.error(
          "Internal Vidarr error: the workflow run priority must be a value between 1 and 4.");
    } else {
      SimpleEntry thisworkflowrun = new SimpleEntry(vidarrId, this.priority);
      if (this.priority >= state.waiting.last().getValue()) {
        state.waiting.remove(thisworkflowrun);
        currentInWaitingCount.labels(workflowName).set(state.waiting.size());
        return ConsumableResourceResponse.AVAILABLE;
      } else {
        state.waiting.add(thisworkflowrun);
        currentInWaitingCount.labels(workflowName).set(state.waiting.size());
        return ConsumableResourceResponse.error(
            String.format("There are workflows currently running with higher prioirity.", workflowName));
      }
    }
  }

  @Override
  public void startup(String name) {
    // Always ok.
  }

}
