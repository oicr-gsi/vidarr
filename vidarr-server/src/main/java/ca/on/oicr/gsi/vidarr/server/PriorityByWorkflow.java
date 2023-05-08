package ca.on.oicr.gsi.vidarr.server;

import static java.util.Map.Entry.comparingByValue;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.databind.JsonNode;
import io.prometheus.client.Gauge;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;


final class PriorityByWorkflow implements ConsumableResource {

  private List acceptedPriorities = Arrays.asList(1, 2, 3, 4);

  private static final class WaitingState {

    private final SortedSet<SimpleEntry<String, Integer>> waiting = new ConcurrentSkipListSet<SimpleEntry<String, Integer>>(comparingByValue()){

      @Override
      public boolean add(SimpleEntry<String, Integer> simpleEntry) {
        super.remove(this.getByKey(simpleEntry.getKey()));
        return super.add(simpleEntry);
      }

      public boolean removeByKey(String simpleEntryKey) {
        for (SimpleEntry entry : this) {
          if (entry.getKey().equals(simpleEntryKey)) {
            super.remove(this.getByKey(entry.getKey().toString()));
            return true;
          }
        }
        return false;
      }

      public SimpleEntry getByKey(String simpleEntryKey){
        for (SimpleEntry entry : this){
          if (entry.getKey().equals(simpleEntryKey)) {return(entry);}
        }
        return null;
      }
    };
  }


  //remove?
  private static final Gauge currentInPriorityWaitingCount =
      Gauge.build(
              "vidarr_in_priority_waiting_per_workflow_current",
              "The current number of workflows that are on priority waiting to run.")
          .labelNames("workflow")
          .register();

  private final Map<String, WaitingState> workflows = new ConcurrentHashMap<>();

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return Optional.of(new Pair<String, BasicType>("Priority", BasicType.INTEGER));
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId, Optional<Map<String, JsonNode>> resourceJson) {
    final var stateWaiting = workflows.computeIfAbsent(workflowName, k -> new WaitingState()).waiting;
    // since we just created it if it doesn't exist, no need for null check here

    //how to address this
    //stateWaiting.add(new SimpleEntry(vidarrId, this.priority));
    currentInPriorityWaitingCount.labels(workflowName).set(stateWaiting.size());
  }

  @Override
  public void release(String workflowName, String workflowVersion, String vidarrId) {
    final var state = workflows.get(workflowName);
    if (state != null) {
      //replace with removeByKey
      state.waiting.remove(vidarrId);
      currentInPriorityWaitingCount.labels(workflowName).set(state.waiting.size());
    }
  }

  @Override
  public synchronized ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {

    int workflowPriority;
    if (input.isEmpty()) {
      workflowPriority = 1;
    } else {
      JsonNode nodeInput = input.get();
      workflowPriority = nodeInput.get("Priority").asInt();
    }

    if (!acceptedPriorities.contains(workflowPriority)){
      return ConsumableResourceResponse.error(
          String.format("Vidarr error: The workflow %s run priority must be a value between 1 and 4.", workflowName));
    }
    final var state = workflows.get(workflowName);
    if (state == null) {
      return ConsumableResourceResponse.error(
          String.format("Internal Vidarr error: The %s workflow run priority has not been configured properly.", workflowName));
    } else {
      if (workflowPriority >= state.waiting.last().getValue()) {
        state.waiting.remove(new SimpleEntry(vidarrId, workflowPriority));
        currentInPriorityWaitingCount.labels(workflowName).set(state.waiting.size());
        return ConsumableResourceResponse.AVAILABLE;
      } else {
        state.waiting.add(new SimpleEntry(vidarrId, workflowPriority));
        currentInPriorityWaitingCount.labels(workflowName).set(state.waiting.size());
        return ConsumableResourceResponse.error(
            String.format("There are %s workflows currently queued up with higher priority.", workflowName));
      }
    }
  }

  @Override
  public void startup(String name) {
    // Always ok.
  }

  public void set(String workflowName, String vidarrId, int priority) {}

}
