package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import java.util.function.Predicate;

final class OperationStepRequire<Result> extends OperationStep<Result, Result> {

  private final String failureMessage;
  private final Predicate<Result> success;

  public OperationStepRequire(Predicate<Result> success, String failureMessage) {
    super();
    this.success = success;
    this.failureMessage = failureMessage;
  }

  @Override
  public <State extends Record, TX> void run(
      Result result,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Result> next) {
    boolean wasSuccess;
    try {
      wasSuccess = success.test(result);
    } catch (Exception e) {
      next.error(e.getMessage());
      return;
    }
    if (wasSuccess) {
      next.next(result);
    } else {
      next.error(failureMessage);
    }
  }
}
