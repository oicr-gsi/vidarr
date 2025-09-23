package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityScorer;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class ResourceOptimizingPriorityScorer implements PriorityScorer {

  protected record WorkflowRunScore(String workflowName, String vidarrId,
                                    int currentPriority, int originalPriority) implements
      Comparable<WorkflowRunScore> {

    @Override
    public int compareTo(WorkflowRunScore workflowRunScore) {
      int result = Integer.compare(workflowRunScore.currentPriority, this.currentPriority);
      if (result == 0) {
        result = this.vidarrId.compareTo(workflowRunScore.vidarrId);
      }
      return result;
    }
  }

  private final SortedSet<WorkflowRunScore> active = new TreeSet<>();
  private boolean useCustom;
  private int globalMaxInFlight, maxInFlightPerWorkflow;

  @Override
  public boolean compute(String workflowName, String workflowVersion, String vidarrId,
      Instant created, OptionalInt workflowMaxInFlight, int score) {
    // You can't just declare yourself inflight
    if (score == Integer.MAX_VALUE) {
      score--;
    }
    final int finalScore = score;
    final int workflowLimit = getWorkflowInFlightLimit(workflowMaxInFlight);

    synchronized (active) {
      // Check if the record already exists
      final Optional<WorkflowRunScore> existing = active.stream()
          .filter(e -> e.vidarrId().equals(vidarrId)).findFirst();
      if (existing.isPresent()) {
        final int existingPriority = existing.get().currentPriority();

        // If you're already inflight, we can't rescind the resource
        if (existingPriority == Integer.MAX_VALUE) {
          return true;
        }

        // We recognize you and know you've been scored correctly.
        else if (existingPriority == score) {
          // return whether this workflow run may launch
          return attemptLaunch(existing.get(), workflowLimit, finalScore);
        }

        // Your score has changed, so we need to re-evaluate you below.
        else {
          active.remove(existing.get());
        }
      }

      // Existing was not present or score has changed so score was removed, re-evaluate.
      // Checking that an existing record was present ensures that we don't allow first-come low
      // priority jobs to take all the tokens
      final WorkflowRunScore wfrScore = new WorkflowRunScore(workflowName,
          vidarrId, score, score);
      active.add(wfrScore);
      if (existing.isPresent()) {
        // Now attempt to launch after adding updated wfrScore, this may have changed the queue
        return attemptLaunch(wfrScore, workflowLimit, finalScore);
      } else {
        return false;
      }
    }
  }

  private boolean attemptLaunch(WorkflowRunScore workflowRunScore, int workflowMaxInFlight,
      int finalScore) {
    final boolean globalRunningRoom =
        active.stream().filter(e -> e.currentPriority() > finalScore).count()
            < globalMaxInFlight;

    if (globalRunningRoom) {
      // Check if there is per-workflow max in flight running room
      final boolean workflowRunningRoom =
          getActiveByWorkflow(workflowRunScore.workflowName()).stream()
              .filter(e -> e.currentPriority() > finalScore).count()
              < workflowMaxInFlight;
      if (workflowRunningRoom) {
        // Both global and per-workflow max in flight have room for you, set to inflight
        active.remove(workflowRunScore);
        active.add(
            new WorkflowRunScore(workflowRunScore.workflowName(),
                workflowRunScore.vidarrId(), Integer.MAX_VALUE,
                finalScore));

        // Recalculate per-workflow inflight after this change. If this workflow has hit its
        // max in flight, set all non-inflight priorities to 0.
        if (getActiveByWorkflow(workflowRunScore.workflowName()).stream()
            .filter(e -> e.currentPriority() == Integer.MAX_VALUE).count()
            == workflowMaxInFlight) {
          for (WorkflowRunScore wrs : new TreeSet<>(getActiveByWorkflow(
              workflowRunScore.workflowName()))) {
            if (wrs.currentPriority != Integer.MAX_VALUE) {
              active.remove(wrs);
              active.add(
                  new WorkflowRunScore(wrs.workflowName, wrs.vidarrId, 0,
                      wrs.originalPriority));
            }
          }
        }

        // Either way, it's in flight now, this resource is passing.
        return true;
      }

      // Even though there is global running room, this workflow has hit its max in flight,
      // so you can't run.
      else {
        return false;
      }
    }

    // Global max in flight has been hit, you can't run.
    else {
      return false;
    }
  }


  @Override
  public Optional<HttpHandler> httpHandler() {
    return Optional.empty();
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId) {
    synchronized (active) {
      active.removeIf(a -> a.vidarrId().equals(vidarrId));
      active.add(new WorkflowRunScore(workflowName, vidarrId, Integer.MAX_VALUE,
          Integer.MAX_VALUE));
    }
  }

  @Override
  public void complete(String workflowName, String workflowVersion, String vidarrId) {
    synchronized (active) {
      active.removeIf(a -> a.vidarrId().equals(vidarrId));
      resetWorkflowQueue(workflowName);
    }
  }

  @Override
  public void putItBack(String workflowName, String workflowVersion, String vidarrId) {
    synchronized (active) {
      Optional<WorkflowRunScore> wfrScoreMaybe = active.stream()
          .filter(a -> a.vidarrId().equals(vidarrId)).findFirst();
      if (wfrScoreMaybe.isPresent()) {
        WorkflowRunScore wfrScore = wfrScoreMaybe.get();
        active.remove(wfrScore);
        active.add(new WorkflowRunScore(workflowName, vidarrId,
            wfrScore.originalPriority(),
            wfrScore.originalPriority()));

        resetWorkflowQueue(workflowName);
      } // else do nothing - but this shouldn't happen
    }
  }

  private void resetWorkflowQueue(String workflowName) {
    // Reset all of this workflow's waiting runs to their original priority. We don't know
    // the per-workflow max in flight here, so we have to do this every time :(
    SortedSet<WorkflowRunScore> workflowActive = getActiveByWorkflow(workflowName);
    for (WorkflowRunScore score : workflowActive) {
      if (score.currentPriority() != Integer.MAX_VALUE) {
        active.remove(score);
        active.add(
            new WorkflowRunScore(score.workflowName(), score.vidarrId(),
                score.originalPriority(), score.originalPriority()));
      }
    }
  }


  @Override
  public void startup() {
    // Do nothing
  }

  private SortedSet<WorkflowRunScore> getActiveByWorkflow(String workflowName) {
    return active.stream().filter(
            s -> s.workflowName.equals(workflowName))
        .collect(Collectors.toCollection(TreeSet::new));
  }


  private int getWorkflowInFlightLimit(OptionalInt customWorkflowMaxInFlight) {
    return useCustom ? customWorkflowMaxInFlight.orElse(maxInFlightPerWorkflow)
        : maxInFlightPerWorkflow;
  }


  public int getGlobalMaxInFlight() {
    return globalMaxInFlight;
  }

  public void setGlobalMaxInFlight(int globalMaxInFlight) {
    this.globalMaxInFlight = globalMaxInFlight;
  }

  /**
   * This is the static number set in the config. For the calculated value, use
   * getWorkflowInFlightLimit().
   */
  public int getMaxInFlightPerWorkflow() {
    return maxInFlightPerWorkflow;
  }

  public void setMaxInFlightPerWorkflow(int maxInFlightPerWorkflow) {
    this.maxInFlightPerWorkflow = maxInFlightPerWorkflow;
  }

  public boolean isUseCustom() {
    return useCustom;
  }

  public void setUseCustom(boolean useCustom) {
    this.useCustom = useCustom;
  }
}
