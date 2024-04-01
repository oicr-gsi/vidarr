package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class OperationStepSleep<Result> extends OperationStep<Result, Result> {

  private final Duration duration;

  public OperationStepSleep(Duration duration) {
    super();
    this.duration = duration;
  }

  @Override
  public <State extends Record, TX> void run(
      Result result,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Result> next) {
    transactionManager.scheduleTask(
        duration.get(TimeUnit.SECONDS.toChronoUnit()), TimeUnit.SECONDS, () -> next.next(result));
  }
}
