package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import io.prometheus.client.Counter;
import java.util.function.Predicate;

final class OperationStepMonitor<Value> extends OperationStep<Value, Value> {

  private final Counter counter;
  private final String[] labels;
  private final Predicate<Value> success;

  OperationStepMonitor(Counter counter, Predicate<Value> success, String[] labels) {
    this.counter = counter;
    this.success = success;
    this.labels = labels;
  }

  @Override
  public <State extends Record, TX> void run(
      Value input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Value> next) {
    try {
      if (success.test(input)) {
        counter.labels(labels).inc();
      }
    } catch (Exception e) {
      next.error(e.getMessage());
      return;
    }
    next.next(input);
  }
}
