package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.WorkMonitor;
import ca.on.oicr.gsi.vidarr.WorkMonitor.Status;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/**
 * A workflow monitor that buffers request until later
 *
 * @param <T> the output type
 * @param <S> the recovery state type
 */
final class DelayWorkMonitor<T, S> implements WorkMonitor<T, S> {
  private WorkMonitor<T, S> delegate;
  private final Deque<Runnable> waiting = new ConcurrentLinkedDeque<>();

  public void complete(T result) {
    executeSafely(() -> delegate.complete(result));
  }

  private void executeSafely(Runnable task) {
    boolean run;
    synchronized (this) {
      if (delegate == null) {
        waiting.add(task);
        run = false;
      } else {
        run = true;
      }
    }
    if (run) {
      task.run();
    }
  }

  @Override
  public void log(System.Logger.Level level, String message) {
    executeSafely(() -> delegate.log(level, message));
  }

  public void permanentFailure(String reason) {
    executeSafely(() -> delegate.permanentFailure(reason));
  }

  public void scheduleTask(long delay, TimeUnit units, Runnable task) {
    executeSafely(() -> delegate.scheduleTask(delay, units, task));
  }

  @Override
  public void storeDebugInfo(JsonNode information) {
    executeSafely(() -> delegate.storeDebugInfo(information));
  }

  public void scheduleTask(Runnable task) {
    executeSafely(() -> delegate.scheduleTask(task));
  }

  /**
   * Add a real work monitor that will handle the work
   *
   * @param monitor the monitor to use
   */
  public void set(WorkMonitor<T, S> monitor) {
    synchronized (this) {
      if (delegate == null) {
        delegate = monitor;
      } else {
        throw new IllegalStateException("Cannot replace work monitor once activated.");
      }
    }
    Runnable task;
    while ((task = waiting.pollFirst()) != null) {
      task.run();
    }
  }

  public void storeRecoveryInformation(S state) {
    executeSafely(() -> delegate.storeRecoveryInformation(state));
  }

  public void updateState(Status status) {
    executeSafely(() -> delegate.updateState(status));
  }
}
