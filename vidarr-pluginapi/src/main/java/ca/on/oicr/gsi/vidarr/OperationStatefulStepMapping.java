package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;

final class OperationStatefulStepMapping<
        State extends Record, OriginalState extends Record, Input, Output>
    extends OperationStatefulStep<State, State, OriginalState, Input, Output> {

  private final StatefulTransformer<State, Input, Output> transformer;

  OperationStatefulStepMapping(StatefulTransformer<State, Input, Output> transformer) {
    this.transformer = transformer;
  }

  @Override
  State buildState(State state) {
    return state;
  }

  @Override
  JavaType jacksonType(JavaType incoming) {
    return incoming;
  }

  @Override
  State rewind(State state, OperationAction<State, OriginalState, Input> input)
      throws JsonProcessingException {
    return input.rewind(state);
  }

  @Override
  public <TX> void run(
      OperationAction<State, OriginalState, Input> input,
      State nextState,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Output> next) {
    input.run(
        nextState,
        operation,
        transactionManager,
        new OperationControlFlow<>() {
          @Override
          public void cancel() {
            next.cancel();
          }

          @Override
          public void error(String error) {
            next.error(error);
          }

          @Override
          public void next(Input input) {
            if (!operation.isLive()) {
              next.cancel();
              return;
            }

            Output output;
            try {
              output = transformer.transform(nextState, input);
            } catch (Exception e) {
              next.error(e.getMessage());
              return;
            }
            next.next(output);
          }

          @Override
          public JsonNode serializeNestedState(State state) {
            return next.serializeNestedState(state);
          }
        });
  }
}
