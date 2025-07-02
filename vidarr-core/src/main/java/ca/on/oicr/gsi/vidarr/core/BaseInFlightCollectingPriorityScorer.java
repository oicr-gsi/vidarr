package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityScorer;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SortedSet;

public abstract class BaseInFlightCollectingPriorityScorer implements PriorityScorer {
  protected record WorkflowRunScore(String vidarrId, int priority)
      implements Comparable<WorkflowRunScore> {

    @Override
    public int compareTo(WorkflowRunScore workflowRunScore) {
      int result = Integer.compare(workflowRunScore.priority, this.priority);
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
      final Optional<WorkflowRunScore> existing = active.stream().filter(e -> e.vidarrId().equals(vidarrId)).findFirst();
      if (existing.isPresent()) {
        final int existingPriority = existing.get().priority();
        if (existingPriority == Integer.MAX_VALUE) {
          return true;
        } else if (existingPriority == score) {
          if (active.headSet(existing.get()).size() < limit) {
            active.remove(existing.get());
            active.add(new WorkflowRunScore(vidarrId, Integer.MAX_VALUE));
            return true;
          } else {
            return false;
          }

        } else {
          active.remove(existing.get());
        }
      }
      final WorkflowRunScore wfrScore = new WorkflowRunScore(vidarrId, score);
      active.add(wfrScore);
      // Checking that an existing record was present ensures that we don't allow first-come low
      // priority jobs to take all the tokens
      if (existing.isPresent() && active.headSet(wfrScore).size() < limit) {
        active.remove(wfrScore);
        active.add(new WorkflowRunScore(vidarrId, Integer.MAX_VALUE));
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
      active.add(new WorkflowRunScore(vidarrId, Integer.MAX_VALUE));
    }
  }

  @Override
  public final void release(String workflowName, String workflowVersion, String vidarrId) {
    final SortedSet<WorkflowRunScore> active = get(workflowName, workflowVersion);
    synchronized (active) {
      active.removeIf(a -> a.vidarrId().equals(vidarrId));
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
