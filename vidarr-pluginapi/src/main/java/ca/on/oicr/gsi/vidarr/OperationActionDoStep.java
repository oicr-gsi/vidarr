package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;

final class OperationActionDoStep<State extends Record, OriginalState extends Record, Input, Output>
    extends OperationAction<State, OriginalState, Output> {

  private final OperationAction<State, OriginalState, Input> input;
  private final OperationStep<Input, Output> step;

  public OperationActionDoStep(
      OperationAction<State, OriginalState, Input> input, OperationStep<Input, Output> step) {
    super();
    this.input = input;
    this.step = step;
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
      OperationControlFlow<State, Output> flow) {
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
          public void next(Input input) {
            if (!operation.isLive()) {
              flow.cancel();
              return;
            }

            step.run(input, operation, transactionManager, flow);
          }

          @Override
          public JsonNode serializeNestedState(State state) {
            return flow.serializeNestedState(state);
          }
        });
  }
}
