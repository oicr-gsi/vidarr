package ca.on.oicr.gsi.vidarr.core;

import java.util.OptionalInt;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public final class InFlightCollectingPriorityScorer extends BaseInFlightCollectingPriorityScorer {

  private final SortedSet<WorkflowRunScore> active = new TreeSet<>();

  /**
   * The maximum number of workflows which may occupy the global max-in-flight limit before those
   * workflows get deprioritized to let something else run
   */
  private int hogFactor;

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

  @Override
  protected void preprocessActive(int limit) {
    if (hogFactor > 0 && active.size() > limit) {
      synchronized (active) {
        SortedSet<WorkflowRunScore> activeHead = new TreeSet<>();

        // It's so cool how there's seemingly no call to do this with a sorted set
        int i = 0;
        while (i < limit) {
          activeHead.add(active.first());
          active.remove(active.first());
          i++;
        }
        active.addAll(activeHead);

        // If everything is inflight, there's no running room. Exit early
        // Alternatively, if nothing is inflight, this is also a waste of time. Exit early
        if (activeHead.stream()
            .allMatch(s -> s.currentPriority() == Integer.MAX_VALUE)
            || activeHead.stream()
            .noneMatch(s -> s.currentPriority() == Integer.MAX_VALUE)) {
          return;
        }

        // If the number of workflows present in the priority queue up to the limit are just a few
        // (less or equal to the hog factor) then we need to devalue the waiting jobs of those
        // workflows to let something else through
        Set<String> uniqueWorkflows = activeHead.stream().map(WorkflowRunScore::workflowName)
            .collect(Collectors.toSet());
        if (uniqueWorkflows.size() <= hogFactor) {
          // can't manipulate active while iterating over it
          Set<WorkflowRunScore> toRemove = new TreeSet<>(), toAdd = new TreeSet<>();
          for (WorkflowRunScore score : active) {
            if (uniqueWorkflows.contains(score.workflowName())
                && score.currentPriority() != Integer.MAX_VALUE) {
              toRemove.add(score);
              toAdd.add(new WorkflowRunScore(score.workflowName(), score.workflowVersion(),
                  score.vidarrId(), 0,
                  score.originalPriority()));
            }
          }
          active.removeAll(toRemove);
          active.addAll(toAdd);
        }
      }
    }
  }

  @Override
  protected void postprocessActive() {
    // TreeSet implements Cloneable but I can't actually get clone() to work without a million
    // unsafe casts
    // It doesn't hurt anything to do this with hog factor off, just waste of cycles
    if (hogFactor > 0) {
      SortedSet<WorkflowRunScore> clonedActive = new TreeSet<>(active);
      for (WorkflowRunScore score : clonedActive) {
        if (score.currentPriority() != Integer.MAX_VALUE) {
          putItBack(score.workflowName(), score.workflowVersion(), score.vidarrId());
        }
      }
    }
  }

  public void setHogFactor(int hogFactor) {
    this.hogFactor = hogFactor;
  }
}
