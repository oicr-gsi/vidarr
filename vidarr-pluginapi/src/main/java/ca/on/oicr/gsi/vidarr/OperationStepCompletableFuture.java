package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import java.util.concurrent.CompletableFuture;

final class OperationStepCompletableFuture<Value>
    extends OperationStep<CompletableFuture<Value>, Value> {

  public OperationStepCompletableFuture() {
    super();
  }

  @Override
  public <State extends Record, TX> void run(
      CompletableFuture<Value> input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Value> next) {
    input.whenComplete(
        (result, throwable) -> {
          if (throwable == null) {
            next.next(result);
          } else {
            next.error(throwable.getMessage());
          }
        });
  }
}
