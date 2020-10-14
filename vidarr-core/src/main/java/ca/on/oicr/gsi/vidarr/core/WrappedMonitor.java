package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.WorkMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A worked monitor that adds extra information during state serialisation
 *
 * @param <A> the type of the added information
 * @param <R> the return type of the inner operation
 * @param <S> the return type of the whole operation
 */
abstract class WrappedMonitor<A, R, S> implements WorkMonitor<R, JsonNode> {
  public interface MonitorConstructor<A, R, S> {
    WrappedMonitor<A, R, S> create(A accessory, WorkMonitor<S, JsonNode> monitor);
  }

  public interface RecoveryStarter<R> {
    void recover(JsonNode state, WorkMonitor<R, JsonNode> monitor);
  }

  /**
   * Restart a task from stored database state
   *
   * @param state the state provided from the database
   * @param accessoryLoader a function to deserialise the database state
   * @param constructor a constructor for the monitor
   * @param task a method to restart a task
   * @param operation the outer operation
   * @param <A> the type of the added information
   * @param <R> the return type of the inner operation
   * @param <S> the return type of the whole operation
   */
  public static <A, R, S> void recover(
      JsonNode state,
      Function<JsonNode, A> accessoryLoader,
      MonitorConstructor<A, R, S> constructor,
      RecoveryStarter<R> task,
      WorkMonitor<S, JsonNode> operation) {
    task.recover(state.get(1), constructor.create(accessoryLoader.apply(state.get(0)), operation));
  }

  /**
   * Modify a task to include extra information
   *
   * @param accessory the extra information to store
   * @param constructor a constructor for the monitor
   * @param task the task to wrap
   * @param <A> the type of the added information
   * @param <R> the return type of the inner operation
   * @param <S> the return type of the whole operation
   * @return a modified task that captures the accessory data
   */
  public static <A, R, S> TaskStarter<S> start(
      A accessory, MonitorConstructor<A, R, S> constructor, TaskStarter<R> task) {
    return (workflowLanguage, workflowId, operation) -> {
      final var array = JsonNodeFactory.instance.arrayNode(2);
      array.insertPOJO(0, accessory);
      array.insertPOJO(
          1, task.start(workflowLanguage, workflowId, constructor.create(accessory, operation)));
      return array;
    };
  }

  private final A accessory;
  private final WorkMonitor<S, JsonNode> monitor;

  public WrappedMonitor(A accessory, WorkMonitor<S, JsonNode> monitor) {
    this.accessory = accessory;
    this.monitor = monitor;
  }

  @Override
  public final void complete(R result) {
    monitor.complete(mix(accessory, result));
  }

  @Override
  public void log(System.Logger.Level level, String message) {
    monitor.log(level, message);
  }

  /**
   * Combine the accessory information and the result from the operation to produce a new output
   *
   * @param accessory the accessory information carried along
   * @param result the result provided by the inner operation
   * @return the result to be provided to the outer operation
   */
  protected abstract S mix(A accessory, R result);

  @Override
  public final void permanentFailure(String reason) {
    monitor.permanentFailure(reason);
  }

  @Override
  public final void scheduleTask(long delay, TimeUnit units, Runnable task) {
    monitor.scheduleTask(delay, units, task);
  }

  @Override
  public final void scheduleTask(Runnable task) {
    monitor.scheduleTask(task);
  }

  @Override
  public final void storeRecoveryInformation(JsonNode state) {
    final var array = JsonNodeFactory.instance.arrayNode(2);
    array.insertPOJO(0, accessory);
    array.insertPOJO(1, state);
    monitor.storeRecoveryInformation(array);
  }

  @Override
  public final void updateState(Status status) {
    monitor.updateState(status);
  }
}
