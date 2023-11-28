package ca.on.oicr.gsi.vidarr.core;

import java.util.OptionalInt;
import java.util.SortedSet;
import java.util.TreeSet;

public final class InFlightCollectingPriorityScorer extends BaseInFlightCollectingPriorityScorer {

  private final SortedSet<WorkflowRunScore> active = new TreeSet<>();

  @Override
  protected SortedSet<WorkflowRunScore> get(String workflowName, String workflowVersion) {
    return active;
  }

  @Override
  protected int getLimit(
      String workflowName,
      String workflowVersion,
      int maxInFlight,
      OptionalInt workflowMaxInFlight) {
    return maxInFlight;
  }
}
