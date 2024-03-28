package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationAction.MAPPER;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep.Child;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;

final class OperationStatefulStepSubStep<
        State extends Record,
        SubState extends Record,
        OriginalState extends Record,
        OriginalSubState extends Record,
        Input,
        Output>
    extends OperationStatefulStep<State, Child<State, SubState>, OriginalState, Input, Output> {

  private final StatefulTransformer<State, Input, OriginalSubState> spawn;
  private final OperationAction<SubState, OriginalSubState, Output> subtask;

  public OperationStatefulStepSubStep(
      StatefulTransformer<State, Input, OriginalSubState> spawn,
      OperationAction<SubState, OriginalSubState, Output> subtask) {
    super();
    this.spawn = spawn;
    this.subtask = subtask;
  }

  @Override
  Child<State, SubState> buildState(State state) {
    return new Child<>(Optional.empty(), state);
  }

  @Override
  JavaType jacksonType(JavaType incoming) {
    return MAPPER
        .getTypeFactory()
        .constructParametricType(Child.class, incoming, subtask.jacksonType());
  }

  @Override
  Child<State, SubState> rewind(
      Child<State, SubState> state, OperationAction<State, OriginalState, Input> input)
      throws JsonProcessingException {
    return new Child<>(Optional.empty(), input.rewind(state.state()));
  }

  @Override
  public <TX> void run(
      OperationAction<State, OriginalState, Input> input,
      Child<State, SubState> state,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<Child<State, SubState>, Output> next) {
    state
        .child()
        .ifPresentOrElse(
            childState ->
                subtask.run(
                    childState,
                    operation,
                    transactionManager,
                    new OperationControlFlow<SubState, Output>() {
                      @Override
                      public void cancel() {
                        next.cancel();
                      }

                      @Override
                      public void error(String error) {
                        next.error(error);
                      }

                      @Override
                      public void next(Output output) {
                        if (!operation.isLive()) {
                          next.cancel();
                          return;
                        }
                        next.next(output);
                      }

                      @Override
                      public JsonNode serializeNestedState(SubState subState) {
                        return next.serializeNestedState(
                            new Child<>(Optional.of(subState), state.state()));
                      }
                    }),
            () ->
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
                        next.error(error);
                      }

                      @Override
                      public void next(Input value) {
                        if (!operation.isLive()) {
                          next.cancel();
                          return;
                        }

                        OriginalSubState originalSubState;
                        try {
                          originalSubState = spawn.transform(state.state(), value);
                        } catch (Exception e) {
                          next.error(e.getMessage());
                          return;
                        }
                        transactionManager.scheduleTask(
                            () -> {
                              if (!operation.isLive()) {
                                next.cancel();
                                return;
                              }
                              final var nextState =
                                  new Child<>(
                                      Optional.of(subtask.buildState(originalSubState)),
                                      state.state());
                              final var recoveryState = next.serializeNestedState(nextState);
                              transactionManager.inTransaction(
                                  tx -> operation.recoveryState(recoveryState, tx));
                              transactionManager.scheduleTask(
                                  () -> run(input, nextState, operation, transactionManager, next));
                            });
                      }

                      @Override
                      public JsonNode serializeNestedState(State state) {
                        return next.serializeNestedState(new Child<>(Optional.empty(), state));
                      }
                    }));
  }
}
