package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import java.lang.System.Logger.Level;

final class OperationStepLog<Value> extends OperationStep<Value, Value> {

  private final Level level;
  private final Transformer<Value, String> message;

  public OperationStepLog(Level level, Transformer<Value, String> message) {
    super();
    this.level = level;
    this.message = message;
  }

  @Override
  public <State extends Record, TX> void run(
      Value input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Value> next) {
    try {
      operation.log(level, message.transform(input));
    } catch (Exception e) {
      next.error(e.getMessage());
      return;
    }
    next.next(input);
  }
}
