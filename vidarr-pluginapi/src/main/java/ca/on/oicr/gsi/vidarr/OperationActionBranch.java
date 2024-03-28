package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import ca.on.oicr.gsi.vidarr.OperationAction.BranchState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

final class OperationActionBranch<Output>
    extends OperationAction<BranchState, BranchState, Output> {

  private final Map<String, OperationAction<?, ?, Output>> branches;

  OperationActionBranch(Map<String, OperationAction<?, ?, Output>> branches) {
    this.branches = branches;
  }

  @Override
  BranchState buildState(BranchState branchState) {
    return branchState;
  }

  @Override
  public BranchState deserializeOriginal(JsonNode originalState) throws JsonProcessingException {
    return MAPPER.treeToValue(originalState, BranchState.class);
  }

  @Override
  JavaType jacksonType() {
    return MAPPER.getTypeFactory().constructType(BranchState.class);
  }

  @Override
  BranchState rewind(BranchState state) throws JsonProcessingException {
    return new BranchState(state.name(), rewind(branches.get(state.name()), state.inner()));
  }

  private <State extends Record> JsonNode rewind(
      OperationAction<State, ?, Output> action, JsonNode inner) throws JsonProcessingException {
    return MAPPER.valueToTree(action.rewind(MAPPER.treeToValue(inner, action.jacksonType())));
  }

  @Override
  <TX> void run(
      BranchState branchState,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<BranchState, Output> flow) {
    final var branch = branches.get(branchState.name());
    if (branch == null) {
      flow.error(
          String.format("Branch is looking for path %s, but this is unknown", branchState.name()));
    } else {
      startInner(
          branch,
          branchState.name(),
          MAPPER.convertValue(branchState.inner(), branch.jacksonType()),
          transactionManager,
          operation,
          flow);
    }
  }

  <InnerState extends Record, OriginalInnerState extends Record, TX> void startInner(
      OperationAction<InnerState, OriginalInnerState, Output> action,
      String name,
      InnerState innerState,
      TransactionManager<TX> transactionManager,
      ActiveOperation<TX> operation,
      OperationControlFlow<BranchState, Output> next) {
    action.run(
        innerState,
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
          public void next(Output output) {
            next.next(output);
          }

          @Override
          public JsonNode serializeNestedState(InnerState innerState) {
            return next.serializeNestedState(new BranchState(name, MAPPER.valueToTree(innerState)));
          }
        });
  }
}
