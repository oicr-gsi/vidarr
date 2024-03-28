package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;

final class OperationActionReload<State extends Record, OriginalState extends Record, Input, Result>
    extends OperationAction<State, OriginalState, Result> {

  private final OperationAction<State, OriginalState, Input> input;
  private final Loader<State, Result> loader;

  OperationActionReload(
      OperationAction<State, OriginalState, Input> input, Loader<State, Result> loader) {
    this.input = input;
    this.loader = loader;
  }

  @Override
  State buildState(OriginalState originalState) {
    return input.buildState(originalState);
  }

  @Override
  public OriginalState deserializeOriginal(JsonNode originalState) throws JsonProcessingException {
    return input.deserializeOriginal(originalState);
  }

  @Override
  JavaType jacksonType() {
    return input.jacksonType();
  }

  @Override
  State rewind(State state) throws JsonProcessingException {
    return input.rewind(state);
  }

  @Override
  <TX> void run(
      State state,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Result> flow) {
    input.run(
        state,
        operation,
        transactionManager,
        new OperationControlFlow<>() {
          @Override
          public void cancel() {
            flow.cancel();
          }

          @Override
          public void error(String error) {
            flow.error(error);
          }

          @Override
          public void next(Input value) {
            if (!operation.isLive()) {
              flow.cancel();
              return;
            }
            Result output;
            try {
              output = loader.prepare(state);
            } catch (Exception e) {
              flow.error(e.getMessage());
              return;
            }
            flow.next(output);
          }

          @Override
          public JsonNode serializeNestedState(State state) {
            return flow.serializeNestedState(state);
          }
        });
  }
}
