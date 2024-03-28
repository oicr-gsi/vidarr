package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;

final class OperationStepMapping<Input, Output> extends OperationStep<Input, Output> {

  private final Transformer<Input, Output> transformer;

  OperationStepMapping(Transformer<Input, Output> transformer) {
    this.transformer = transformer;
  }

  @Override
  public <State extends Record, TX> void run(
      Input input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Output> next) {
    final Output result;
    try {
      result = transformer.transform(input);
    } catch (Exception e) {
      next.error(e.getMessage());
      return;
    }
    next.next(result);
  }
}
