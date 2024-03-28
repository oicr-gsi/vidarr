package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.databind.JsonNode;

final class OperationStepDebugInfo<Value> extends OperationStep<Value, Value> {

  private final Transformer<Value, JsonNode> debugInfo;

  public OperationStepDebugInfo(Transformer<Value, JsonNode> debugInfo) {
    this.debugInfo = debugInfo;
  }

  @Override
  public <State extends Record, TX> void run(
      Value input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Value> next) {
    final JsonNode newDebugInfo;
    try {
      newDebugInfo = debugInfo.transform(input);
    } catch (Exception e) {
      next.error(e.getMessage());
      return;
    }
    transactionManager.inTransaction(tx -> operation.debugInfo(newDebugInfo, tx));
    next.next(input);
  }
}
