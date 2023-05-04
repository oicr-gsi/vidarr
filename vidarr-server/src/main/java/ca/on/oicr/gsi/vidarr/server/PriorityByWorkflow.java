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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;


final class PriorityByWorkflow implements ConsumableResource {

  private Integer priority;

  private List acceptedPriorities = Arrays.asList(1, 2, 3, 4);

  private static final class WaitingState {

    private final SortedSet<SimpleEntry<String, Integer>> waiting = new TreeSet<SimpleEntry<String, Integer>>(
        new PairComparator()){

      @Override
      public boolean add(SimpleEntry<String, Integer> simpleEntry) {
        super.remove(this.containsKey(simpleEntry));
        return super.add(simpleEntry);
      }

      public SimpleEntry containsKey(SimpleEntry simpleEntry){
        String entryKey = (String) simpleEntry.getKey();
        for (SimpleEntry entry : this){
          if (entry.getKey().equals(entryKey)) {return(entry);}
        }
        return null;
      }
    };
  }

  static class PairComparator implements Comparator<SimpleEntry<String, Integer>> {

    @Override
    public int compare(SimpleEntry<String, Integer> entry1, SimpleEntry<String, Integer> entry2) {
      return entry1.getValue() - entry2.getValue();
    }

  }

  //remove?
  private static final Gauge currentInWaitingCount =
      Gauge.build(
              "vidarr_in_waiting_per_workflow_current",
              "The current number of workflows that are on priority waiting to run.")
          .labelNames("workflow")
          .register();

  private final Map<String, WaitingState> workflows = new ConcurrentHashMap<>();

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return Optional.of(new Pair<String, BasicType>("Priority", BasicType.INTEGER));
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
      state.waiting.remove(new SimpleEntry(vidarrId, this.priority));
      currentInWaitingCount.labels(workflowName).set(state.waiting.size());
    }
  }

  @Override
  public synchronized ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {

    if (input.isEmpty()) {
      this.priority = 1;
    } else {
      JsonNode nodeInput = input.get();
      this.priority = nodeInput.get("Priority").asInt();
    }

    if (!acceptedPriorities.contains(this.priority)){
      return ConsumableResourceResponse.error(
          String.format("Vidarr error: The workflow %s run priority must be a value between 1 and 4.", workflowName));
    }
    final var state = workflows.get(workflowName);
    if (state == null) {
      return ConsumableResourceResponse.error(
          String.format("Internal Vidarr error: The %s workflow run priority has not been configured properly.", workflowName));
    } else {
      if (this.priority >= state.waiting.last().getValue()) {
        state.waiting.remove(new SimpleEntry(vidarrId, this.priority));
        currentInWaitingCount.labels(workflowName).set(state.waiting.size());
        return ConsumableResourceResponse.AVAILABLE;
      } else {
        state.waiting.add(new SimpleEntry(vidarrId, this.priority));
        currentInWaitingCount.labels(workflowName).set(state.waiting.size());
        return ConsumableResourceResponse.error(
            String.format("There are %s workflows currently queued up with higher priority.", workflowName));
      }
    }
  }

  @Override
  public void startup(String name) {
    // Always ok.
  }

  public void set(String workflowName, String vidarrId, int priority) {
    this.priority = priority;
  }

}
