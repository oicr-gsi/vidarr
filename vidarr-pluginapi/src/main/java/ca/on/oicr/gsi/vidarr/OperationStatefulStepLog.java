package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import java.lang.System.Logger.Level;

final class OperationStatefulStepLog<State extends Record, OriginalState extends Record, Value>
    extends OperationStatefulStep<State, State, OriginalState, Value, Value> {

  private final StatefulTransformer<State, Value, String> fetch;
  private final Level level;

  public OperationStatefulStepLog(Level level, StatefulTransformer<State, Value, String> fetch) {
    super();
    this.level = level;
    this.fetch = fetch;
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

            String message;
            try {
              message = fetch.transform(nextState, value);
            } catch (Exception e) {
              next.error(e.getMessage());
              return;
            }
            operation.log(level, message);
            next.next(value);
          }

          @Override
          public JsonNode serializeNestedState(State state) {
            return next.serializeNestedState(state);
          }
        });
  }
}
