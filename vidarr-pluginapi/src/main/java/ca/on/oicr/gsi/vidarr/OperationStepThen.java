package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.databind.JsonNode;

final class OperationStepThen<Input, Intermediate, Output> extends OperationStep<Input, Output> {

  private final OperationStep<Input, Intermediate> first;
  private final OperationStep<Intermediate, Output> second;

  public OperationStepThen(
      OperationStep<Input, Intermediate> first, OperationStep<Intermediate, Output> second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public <State extends Record, TX> void run(
      Input input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Output> next) {
    first.run(
        input,
        operation,
        transactionManager,
        new OperationControlFlow<State, Intermediate>() {
          @Override
          public void cancel() {
            next.cancel();
          }

          @Override
          public void error(String error) {
            next.error(error);
          }

          @Override
          public void next(Intermediate output) {
            second.run(output, operation, transactionManager, next);
          }

          @Override
          public JsonNode serializeNestedState(State state) {
            return next.serializeNestedState(state);
          }
        });
  }
}
