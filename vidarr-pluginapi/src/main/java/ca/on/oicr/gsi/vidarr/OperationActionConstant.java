package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;

final class OperationActionConstant<State extends Record, Output>
    extends OperationAction<State, State, Output> {

  private final JavaType type;
  private final Output value;

  OperationActionConstant(Class<State> type, Output value) {
    this.value = value;
    this.type = MAPPER.constructType(type);
  }

  OperationActionConstant(TypeReference<State> type, Output value) {
    this.value = value;
    this.type = MAPPER.constructType(type);
  }

  @Override
  State buildState(State state) {
    return state;
  }

  @Override
  public State deserializeOriginal(JsonNode originalState) throws JacksonException {
    return MAPPER.treeToValue(originalState, type);
  }

  @Override
  JavaType jacksonType() {
    return type;
  }

  @Override
  State rewind(State state) throws JacksonException {
    return state;
  }

  @Override
  <TX> void run(
      State state,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Output> flow) {
    flow.next(value);
  }
}
