package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityScorer;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        final int existingOriginalPriority = existing.get().originalPriority();
        final int existingCurrentPriority = existing.get().currentPriority();

        // If your priority is 0, skip all this complicated stuff downstream
        // but still update originalPriority if score has changed, for when we eventually reset
        if (existingCurrentPriority == 0) {
          if (existingOriginalPriority != score) {
            active.remove(existing.get());
            active.add(
                new WorkflowRunScore(workflowName, vidarrId, existingCurrentPriority, score));
          }
          return false;
        }

        // If you're already inflight, we can't rescind the resource
        else if (existingCurrentPriority == Integer.MAX_VALUE) {
          return true;
        }

        // We recognize you and know you've been scored correctly.
        else if (existingOriginalPriority == score) {
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
          getActiveByWorkflow(workflowRunScore.workflowName())
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
        if (getActiveByWorkflow(workflowRunScore.workflowName())
            .filter(e -> e.currentPriority() == Integer.MAX_VALUE).count()
            == workflowMaxInFlight) {
          getActiveByWorkflow(workflowRunScore.workflowName()).forEach(wrs -> {
            if (wrs.currentPriority != Integer.MAX_VALUE) {
              active.remove(wrs);
              active.add(
                  new WorkflowRunScore(wrs.workflowName, wrs.vidarrId, 0,
                      wrs.originalPriority));
            }
          });
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

        // We don't want to reset the queue if we rejected this job because workflowMaxInFlight got
        // hit before we tried to launch.
        // If this scorer approved running this job, but some other scorer didn't, we may have
        // set the waiting priorities to 0, reset that only in this case, keep them otherwise
        // so, if we're resetting a job that we put in the running state,
        if (getActiveByWorkflow(workflowName).contains(
            new WorkflowRunScore(workflowName, vidarrId, Integer.MAX_VALUE,
                wfrScore.originalPriority()))) {
          // and we hit the max in flight (ie the rest of the priorities are 0 or inflight)
          if (active.stream().filter(wrs -> wrs.workflowName().equals(workflowName)).allMatch(
              wrs -> wrs.currentPriority() == 0 || wrs.currentPriority() == Integer.MAX_VALUE)) {
            resetWorkflowQueue(workflowName);
          }
        }
      } // else do nothing - but this shouldn't happen
    }
  }

  private void resetWorkflowQueue(String workflowName) {
    getActiveByWorkflow(workflowName).forEach(score -> {
      if (score.currentPriority() != Integer.MAX_VALUE) {
        active.remove(score);
        active.add(
            new WorkflowRunScore(score.workflowName(), score.vidarrId(),
                score.originalPriority(), score.originalPriority()));
      }
    });
  }


  @Override
  public void startup() {
    // Do nothing
  }

  private Stream<WorkflowRunScore> getActiveByWorkflow(String workflowName) {
    return active.stream().filter(
        s -> s.workflowName.equals(workflowName));
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
