package ca.on.oicr.gsi.vidarr.core;

import java.util.Map;
import java.util.OptionalInt;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public final class InFlightCollectingByWorkflowPriorityScorer
    extends BaseInFlightCollectingPriorityScorer {

  private final Map<String, SortedSet<WorkflowRunScore>> active = new ConcurrentHashMap<>();
  private boolean useCustom;

  @Override
  protected SortedSet<WorkflowRunScore> get(String workflowName, String workflowVersion) {
    return active.computeIfAbsent(workflowName, k -> new TreeSet<>());
  }

  @Override
  protected int getLimit(
      String workflowName,
      String workflowVersion,
      int maxInFlight,
      OptionalInt workflowMaxInFlight) {
    return useCustom ? workflowMaxInFlight.orElse(maxInFlight) : maxInFlight;
  }

  public boolean isUseCustom() {
    return useCustom;
  }

  public void setUseCustom(boolean useCustom) {
    this.useCustom = useCustom;
  }
}
