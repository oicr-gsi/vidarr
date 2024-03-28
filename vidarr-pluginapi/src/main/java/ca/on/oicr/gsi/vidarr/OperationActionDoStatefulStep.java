package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;

final class OperationActionDoStatefulStep<
        State extends Record, NextState extends Record, OriginalState extends Record, Input, Output>
    extends OperationAction<NextState, OriginalState, Output> {

  private final OperationStatefulStep<State, NextState, OriginalState, Input, Output> step;
  private final OperationAction<State, OriginalState, Input> input;

  public OperationActionDoStatefulStep(
      OperationAction<State, OriginalState, Input> input,
      OperationStatefulStep<State, NextState, OriginalState, Input, Output> step) {
    super();
    this.input = input;
    this.step = step;
  }

  @Override
  NextState buildState(OriginalState originalState) {
    return step.buildState(input.buildState(originalState));
  }

  @Override
  public OriginalState deserializeOriginal(JsonNode originalState) throws JsonProcessingException {
    return input.deserializeOriginal(originalState);
  }

  @Override
  JavaType jacksonType() {
    return step.jacksonType(input.jacksonType());
  }

  @Override
  NextState rewind(NextState state) throws JsonProcessingException {
    return step.rewind(state, input);
  }

  @Override
  <TX> void run(
      NextState nextState,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<NextState, Output> flow) {
    this.step.run(input, nextState, operation, transactionManager, flow);
  }
}
