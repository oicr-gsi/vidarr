package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;

final class OperationStepStatus<Value> extends OperationStep<Value, Value> {

  private final Transformer<Value, WorkingStatus> status;

  public OperationStepStatus(Transformer<Value, WorkingStatus> status) {
    this.status = status;
  }

  @Override
  public <State extends Record, TX> void run(
      Value input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Value> next) {
    final WorkingStatus newStatus;
    try {
      newStatus = status.transform(input);
    } catch (Exception e) {
      next.error(e.getMessage());
      return;
    }
    transactionManager.inTransaction(tx -> operation.status(OperationStatus.of(newStatus), tx));
    next.next(input);
  }
}
