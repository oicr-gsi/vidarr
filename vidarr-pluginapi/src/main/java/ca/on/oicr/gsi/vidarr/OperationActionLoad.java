package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;

final class OperationActionLoad<State extends Record, Result>
    extends OperationAction<State, State, Result> {

  private final Loader<State, Result> loader;
  private final JavaType type;

  OperationActionLoad(Class<State> type, Loader<State, Result> loader) {
    this.loader = loader;
    this.type = MAPPER.constructType(type);
  }

  OperationActionLoad(TypeReference<State> type, Loader<State, Result> loader) {
    this.loader = loader;
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
      OperationControlFlow<State, Result> flow) {
    if (!operation.isLive()) {
      flow.cancel();
      return;
    }
    Result value;
    try {
      value = loader.prepare(state);
    } catch (Exception e) {
      flow.error(e.getMessage());
      return;
    }
    flow.next(value);
  }
}
