package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationAction.MAPPER;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep.RepeatCounter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class OperationStatefulStepRepeatUntilSuccess<
        State extends Record, OriginalState extends Record, Value>
    extends OperationStatefulStep<State, RepeatCounter<State>, OriginalState, Value, Value> {

  private final Duration delay;
  private final int maximumAttempts;

  public OperationStatefulStepRepeatUntilSuccess(Duration delay, int maximumAttempts) {
    super();
    this.delay = delay;
    this.maximumAttempts = maximumAttempts;
  }

  @Override
  RepeatCounter<State> buildState(State state) {
    return new RepeatCounter<>(0, state);
  }

  @Override
  JavaType jacksonType(JavaType incoming) {
    return MAPPER.getTypeFactory().constructParametricType(RepeatCounter.class, incoming);
  }

  @Override
  RepeatCounter<State> rewind(
      RepeatCounter<State> state, OperationAction<State, OriginalState, Value> input)
      throws JsonProcessingException {
    return new RepeatCounter<>(0, input.rewind(state.state()));
  }

  @Override
  public <TX> void run(
      OperationAction<State, OriginalState, Value> input,
      RepeatCounter<State> state,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<RepeatCounter<State>, Value> next) {
    input.run(
        state.state(),
        operation,
        transactionManager,
        new OperationControlFlow<>() {
          @Override
          public void cancel() {
            next.cancel();
          }

          @Override
          public void error(String error) {
            if (state.attempts() < maximumAttempts) {
              final var nextState = new RepeatCounter<>(state.attempts() + 1, state.state());
              final var recoveryState = next.serializeNestedState(nextState);
              transactionManager.inTransaction(tx -> operation.recoveryState(recoveryState, tx));
              transactionManager.scheduleTask(
                  delay.get(TimeUnit.SECONDS.toChronoUnit()),
                  TimeUnit.SECONDS,
                  () -> run(input, nextState, operation, transactionManager, next));
            } else {
              next.error(error);
            }
          }

          @Override
          public void next(Value value) {
            if (operation.isLive()) {
              next.next(value);

            } else {
              next.cancel();
            }
          }

          @Override
          public JsonNode serializeNestedState(State innerState) {
            return next.serializeNestedState(new RepeatCounter<>(state.attempts(), innerState));
          }
        });
  }
}
