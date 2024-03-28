package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import ca.on.oicr.gsi.vidarr.PollResult.Visitor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class OperationStatefulStepPoll<State extends Record, OriginalState extends Record>
    extends OperationStatefulStep<State, State, OriginalState, PollResult, Void> {

  private final Duration delay;

  public OperationStatefulStepPoll(Duration delay) {
    super();
    this.delay = delay;
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
  State rewind(State state, OperationAction<State, OriginalState, PollResult> input)
      throws JsonProcessingException {
    return input.rewind(state);
  }

  @Override
  public <TX> void run(
      OperationAction<State, OriginalState, PollResult> input,
      State nextState,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Void> next) {
    input.run(
        nextState,
        operation,
        transactionManager,
        new OperationControlFlow<State, PollResult>() {
          @Override
          public void cancel() {
            next.cancel();
          }

          @Override
          public void error(String error) {
            next.error(error);
          }

          @Override
          public void next(PollResult pollResult) {
            if (!operation.isLive()) {
              next.cancel();
              return;
            }
            pollResult.visit(
                new Visitor() {
                  @Override
                  public void active(WorkingStatus status) {
                    transactionManager.inTransaction(
                        transaction -> operation.status(OperationStatus.of(status), transaction));
                    transactionManager.scheduleTask(
                        delay.get(TimeUnit.SECONDS.toChronoUnit()),
                        TimeUnit.SECONDS,
                        () ->
                            OperationStatefulStepPoll.this.run(
                                input, nextState, operation, transactionManager, next));
                  }

                  @Override
                  public void failed(String error) {
                    next.error(error);
                  }

                  @Override
                  public void finished() {
                    next.next(null);
                  }
                });
          }

          @Override
          public JsonNode serializeNestedState(State state) {
            return next.serializeNestedState(state);
          }
        });
  }
}
