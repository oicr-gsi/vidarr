package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.WorkMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.concurrent.TimeUnit;

/**
 * A work monitor with an additional type attribute that gets serialised with the data
 *
 * @param <T> the inner state type
 */
final class MonitorWithType<T> implements WorkMonitor<T, JsonNode> {
  private final WorkMonitor<T, JsonNode> monitor;
  private final String type;

  public MonitorWithType(WorkMonitor<T, JsonNode> monitor, String type) {
    this.monitor = monitor;
    this.type = type;
  }

  @Override
  public void complete(T result) {
    monitor.complete(result);
  }

  @Override
  public void log(System.Logger.Level level, String message) {
    monitor.log(level, message);
  }

  @Override
  public void permanentFailure(String reason) {
    monitor.permanentFailure(reason);
  }

  @Override
  public void scheduleTask(long delay, TimeUnit units, Runnable task) {
    monitor.scheduleTask(delay, units, task);
  }

  @Override
  public void scheduleTask(Runnable task) {
    monitor.scheduleTask(task);
  }

  @Override
  public void storeRecoveryInformation(JsonNode state) {
    final var array = JsonNodeFactory.instance.arrayNode();
    array.add(type);
    array.add(state);
    monitor.storeRecoveryInformation(array);
  }

  @Override
  public void updateState(Status status) {
    monitor.updateState(status);
  }
}
