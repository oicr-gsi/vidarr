package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.BiPredicate;

final class OperationStatefulStepRequire<State extends Record, OriginalState extends Record, Value>
    extends OperationStatefulStep<State, State, OriginalState, Value, Value> {

  private final String failureMessage;
  private final BiPredicate<State, Value> predicate;

  OperationStatefulStepRequire(BiPredicate<State, Value> predicate, String failureMessage) {
    this.predicate = predicate;
    this.failureMessage = failureMessage;
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
  State rewind(State state, OperationAction<State, OriginalState, Value> input)
      throws JsonProcessingException {
    return input.rewind(state);
  }

  @Override
  public <TX> void run(
      OperationAction<State, OriginalState, Value> input,
      State nextState,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Value> next) {
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
          public void next(Value value) {
            if (!operation.isLive()) {
              next.cancel();
              return;
            }

            boolean check;
            try {
              check = predicate.test(nextState, value);
            } catch (Exception e) {
              next.error(e.getMessage());
              return;
            }
            if (check) {
              next.next(value);

            } else {
              next.error(failureMessage);
            }
          }

          @Override
          public JsonNode serializeNestedState(State state) {
            return next.serializeNestedState(state);
          }
        });
  }
}
