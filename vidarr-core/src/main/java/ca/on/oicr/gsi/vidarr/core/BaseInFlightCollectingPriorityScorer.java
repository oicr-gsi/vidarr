package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityScorer;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SortedSet;

public abstract class BaseInFlightCollectingPriorityScorer implements PriorityScorer {

  protected record WorkflowRunScore(String vidarrId, int currentPriority, int originalPriority)
      implements Comparable<WorkflowRunScore> {

    @Override
    public int compareTo(WorkflowRunScore workflowRunScore) {
      int result = Integer.compare(workflowRunScore.currentPriority, this.currentPriority);
      if (result == 0) {
        result = this.vidarrId.compareTo(workflowRunScore.vidarrId());
      }
      return result;
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
    final SortedSet<WorkflowRunScore> active = get(workflowName, workflowVersion);
    final int limit = getLimit(workflowName, workflowVersion, maxInFlight, workflowMaxInFlight);
    synchronized (active) {
      final Optional<WorkflowRunScore> existing = active.stream()
          .filter(e -> e.vidarrId().equals(vidarrId)).findFirst();
      if (existing.isPresent()) {
        final int existingPriority = existing.get().currentPriority();
        if (existingPriority == Integer.MAX_VALUE) {
          return true;
        } else if (existingPriority == score) {
          final int finalScore = score;
          if (active.stream().filter(e -> e.currentPriority() > finalScore).count() < limit) {
            active.remove(existing.get());
            active.add(new WorkflowRunScore(vidarrId, Integer.MAX_VALUE, score));
            return true;
          } else {
            return false;
          }

        } else {
          active.remove(existing.get());
        }
      }
      final WorkflowRunScore wfrScore = new WorkflowRunScore(vidarrId, score, score);
      active.add(wfrScore);
      // Checking that an existing record was present ensures that we don't allow first-come low
      // currentPriority jobs to take all the tokens
      final int finalScore = score;
      if (existing.isPresent()
          && active.stream().filter(e -> e.currentPriority() > finalScore).count() < limit) {
        active.remove(wfrScore);
        active.add(new WorkflowRunScore(vidarrId, Integer.MAX_VALUE, score));
        return true;
      } else {
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
      active.add(new WorkflowRunScore(vidarrId, Integer.MAX_VALUE, Integer.MAX_VALUE));
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
        active.add(new WorkflowRunScore(vidarrId, wfrScore.originalPriority(),
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
