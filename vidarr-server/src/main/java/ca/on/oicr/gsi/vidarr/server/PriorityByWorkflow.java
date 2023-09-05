package ca.on.oicr.gsi.vidarr.server;

import static java.util.Map.Entry.comparingByValue;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.databind.JsonNode;
import io.prometheus.client.Gauge;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public final class PriorityByWorkflow implements ConsumableResource {

  //1 is lowest priority
  //4 is highest priority (will be given access to resources first)
  private List<Integer> acceptedPriorities = Arrays.asList(1, 2, 3, 4);

  public static ConsumableResourceProvider provider() {
    return () -> Stream.of(new Pair<>("priority", PriorityByWorkflow.class));
  }

  private static final class WaitingState {

    private final SortedSet<SimpleEntry<String, Integer>> waiting = new ConcurrentSkipListSet<SimpleEntry<String, Integer>>(comparingByValue()){

      @Override
      public boolean add(SimpleEntry<String, Integer> simpleEntry) {
        SimpleEntry entry = this.getByKey(simpleEntry.getKey());
        if(entry != null){super.remove(entry);}
        return super.add(simpleEntry);
      }

      public SimpleEntry getByKey(String simpleEntryKey){
        for (SimpleEntry entry : this){
          if (entry.getKey().equals(simpleEntryKey)) {return(entry);}
        }
        return null;
      }
    };
  }


  //not currently monitored by anything - potentially remove?
  private static final Gauge currentInPriorityWaitingCount =
      Gauge.build(
              "vidarr_in_priority_waiting_per_workflow_current",
              "The current number of workflows that are on priority waiting to run.")
          .labelNames("workflow")
          .register();

  private final Map<String, WaitingState> workflows = new ConcurrentHashMap<>();

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return Optional.of(new Pair<String, BasicType>("priority", BasicType.INTEGER));
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> resourceJson) {
    // Do nothing, as once the workflow run launches, it is no longer tracked in PriorityByWorkflow
  }

  @Override
  public void release(String workflowName, String workflowVersion, String vidarrId, boolean isLaunched, Optional<JsonNode> input) {
    //If workflow is being throttled by a different consumable resource add to waiting list
    if (!isLaunched){
      set(workflowName, vidarrId, input);
    }
    // Otherwise do nothing
  }

  @Override
  public synchronized ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {

    int workflowPriority = acceptedPriorities.get(0);
    if (input.isPresent()) {
      workflowPriority = input.get().asInt();
    }

    if (!acceptedPriorities.contains(workflowPriority)){
      return ConsumableResourceResponse.error(
          String.format("Vidarr error: The workflow '%s' run's priority (%d) is invalid. Priority "
              + "values should be one of the following: %s", workflowName, workflowPriority,
              acceptedPriorities.stream().map(String::valueOf).collect(Collectors.joining(", "))));
    }


    final var state = workflows.get(workflowName);
    if (state == null || state.waiting.isEmpty()) {
      return ConsumableResourceResponse.AVAILABLE;
    }

    // If this workflow has already been seen
    // Add the current run to the waitlist
    // ensuring it replaces previous runs with the same ID, accounting for if the priority has changed
    set(workflowName, vidarrId, input);
    SimpleEntry resourcePair = new SimpleEntry(vidarrId, workflowPriority);

    if (workflowPriority >= state.waiting.last().getValue()) {
      state.waiting.remove(resourcePair);
      currentInPriorityWaitingCount.labels(workflowName).set(state.waiting.size());
      return ConsumableResourceResponse.AVAILABLE;
    } else {
      currentInPriorityWaitingCount.labels(workflowName).set(state.waiting.size());
      return ConsumableResourceResponse.error(
          String.format("There are %s workflows currently queued up with higher priority.",
              workflowName));
    }


  }

  public void set(String workflowName, String vidarrId, Optional<JsonNode> input) {

    int workflowPriority = Collections.min(acceptedPriorities);
    if (input.isPresent()) {
      workflowPriority = input.get().asInt();
    }

    final var stateWaiting = workflows.computeIfAbsent(workflowName, k -> new WaitingState()).waiting;
    stateWaiting.add(new SimpleEntry(vidarrId, workflowPriority));
    currentInPriorityWaitingCount.labels(workflowName).set(stateWaiting.size());

  }

  @Override
  public void startup(String name) {
    // Always ok.
  }

  @Override
  public boolean isInputFromSubmitterRequired() {
    return false;
  }
}

