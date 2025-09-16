package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityScorer;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SortedSet;

public abstract class BaseInFlightCollectingPriorityScorer implements PriorityScorer {

  protected record WorkflowRunScore(String workflowName, String workflowVersion, String vidarrId,
                                    int currentPriority, int originalPriority)
      implements Comparable<WorkflowRunScore> {

    @Override
    public int compareTo(WorkflowRunScore workflowRunScore) {
      int result = Integer.compare(workflowRunScore.currentPriority, this.currentPriority);
      if (result == 0) {
        result = this.vidarrId.compareTo(workflowRunScore.vidarrId());
      }
      return result;
    }

    @Override
    public String toString() {
      return "WorkflowRunScore[" +
          "workflowName=" + workflowName + ", " +
          "workflowVersion=" + workflowVersion + ", " +
          "vidarrId=" + vidarrId + ", " +
          "currentPriority=" + currentPriority + ", " +
          "originalPriority=" + originalPriority + ']';
    }

  }

  private int maxInFlight;

  @Override
  public final boolean compute(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Instant created,
      OptionalInt workflowMaxInFlight,
      int score) {
    if (score == Integer.MAX_VALUE) {
      score--;
    }
    SortedSet<WorkflowRunScore> active = get(workflowName, workflowVersion);
    final int limit = getLimit(workflowName, workflowVersion, maxInFlight, workflowMaxInFlight);
    synchronized (active) {
      final int finalScore = score;
      preprocessActive(limit);
      final boolean runningRoom =
          active.stream().filter(e -> e.currentPriority() > finalScore).count() < limit;
      final Optional<WorkflowRunScore> existing = active.stream()
          .filter(e -> e.vidarrId().equals(vidarrId)).findFirst();
      if (existing.isPresent()) {
        final int existingPriority = existing.get().currentPriority();
        if (existingPriority == Integer.MAX_VALUE) {
          return true;
        } else if (existingPriority == score) {
          if (runningRoom) {
            active.remove(existing.get());
            active.add(
                new WorkflowRunScore(workflowName, workflowVersion, vidarrId, Integer.MAX_VALUE,
                    score));
            postprocessActive();
            return true;
          } else {
            postprocessActive();
            return false;
          }
        } else {
          active.remove(existing.get());
        }
      }
      final WorkflowRunScore wfrScore = new WorkflowRunScore(workflowName, workflowVersion,
          vidarrId, score, score);
      active.add(wfrScore);
      // Checking that an existing record was present ensures that we don't allow first-come low
      // currentPriority jobs to take all the tokens
      if (existing.isPresent() && runningRoom) {
        active.remove(wfrScore);
        active.add(new WorkflowRunScore(workflowName, workflowVersion, vidarrId, Integer.MAX_VALUE,
            score));
        postprocessActive();
        return true;
      } else {
        postprocessActive();
        return false;
      }
    }
  }

  protected abstract SortedSet<WorkflowRunScore> get(String workflowName, String workflowVersion);

  protected abstract int getLimit(
      String workflowName,
      String workflowVersion,
      int maxInFlight,
      OptionalInt workflowMaxInFlight);

  protected abstract void preprocessActive(int limit);

  protected abstract void postprocessActive();

  public final int getMaxInFlight() {
    return maxInFlight;
  }

  @Override
  public final Optional<HttpHandler> httpHandler() {
    return Optional.empty();
  }

  @Override
  public final void recover(String workflowName, String workflowVersion, String vidarrId) {
    final SortedSet<WorkflowRunScore> active = get(workflowName, workflowVersion);
    synchronized (active) {
      active.removeIf(a -> a.vidarrId().equals(vidarrId));
      active.add(new WorkflowRunScore(workflowName, workflowVersion, vidarrId, Integer.MAX_VALUE,
          Integer.MAX_VALUE));
    }
  }

  @Override
  public final void complete(String workflowName, String workflowVersion, String vidarrId) {
    final SortedSet<WorkflowRunScore> active = get(workflowName, workflowVersion);
    synchronized (active) {
      active.removeIf(a -> a.vidarrId().equals(vidarrId));
    }
  }

  @Override
  public final void putItBack(String workflowName, String workflowVersion, String vidarrId) {
    final SortedSet<WorkflowRunScore> active = get(workflowName, workflowVersion);
    synchronized (active) {
      Optional<WorkflowRunScore> wfrScoreMaybe = active.stream()
          .filter(a -> a.vidarrId().equals(vidarrId)).findFirst();
      if (wfrScoreMaybe.isPresent()) {
        WorkflowRunScore wfrScore = wfrScoreMaybe.get();
        active.remove(wfrScore);
        active.add(new WorkflowRunScore(workflowName, workflowVersion, vidarrId,
            wfrScore.originalPriority(),
            wfrScore.originalPriority()));
      } // else do nothing - but this shouldn't happen
    }
  }

  public final void setMaxInFlight(int maxInFlight) {
    this.maxInFlight = maxInFlight;
  }

  @Override
  public final void startup() {
    // Do nothing
  }
}
