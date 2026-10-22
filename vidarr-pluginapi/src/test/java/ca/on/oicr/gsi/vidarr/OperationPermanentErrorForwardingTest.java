package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationTestDoubles.response;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationAction.BranchState;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep.RepeatCounter;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep.StatefulTransformer;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.lang.System.Logger.Level;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.Test;

/**
 * A permanent failure has to stay permanent all the way to whatever might retry it
 *
 * <p>An action is built out of nested control flows, one per step, and each one decides what to
 * pass on. {@link OperationControlFlow#permanentError(String)} defaults to reporting a retryable
 * error, which is right for a flow with no notion of retrying but wrong for one that merely sits
 * between a step and something that does: a layer that forgets to forward turns "do not bother
 * retrying this" back into "retry this", and the operation spends the whole retry budget on a
 * request that was refused. Every layer that forwards is covered here, because each one forwards
 * separately.
 */
public class OperationPermanentErrorForwardingTest {

  /** The child state for the sub-step cases; only the fact that it round-trips matters. */
  public record SubState(String value) {}

  private static final Duration DELAY = Duration.ofSeconds(7);
  /** A refusal, which {@link OperationStepHandleHttpStatus} reports as permanent. */
  private static final int REFUSED = 403;

  /** A state-aware function that must never run, because the step underneath it refused. */
  private static <State, Input, Output> StatefulTransformer<State, Input, Output> neverCalled() {
    return (state, input) -> {
      throw new AssertionError("the layer's own work must not run after a permanent failure");
    };
  }

  /** The same, for the steps that do not take the state. */
  private static <Input, Output> OperationStep.Transformer<Input, Output> neverCalledStep() {
    return input -> {
      throw new AssertionError("the layer's own work must not run after a permanent failure");
    };
  }

  /** An action that gets as far as a response that repeating the request cannot improve on. */
  private static OperationAction<TestState, TestState, HttpResponse<String>> refused() {
    return OperationAction.load(TestState.class, TestState::value)
        .then(
            OperationStep.<String, HttpResponse<String>>mapping(
                ignored -> response(REFUSED, Map.of())))
        .then(new OperationStepHandleHttpStatus<>());
  }

  private static void assertStillPermanent(RecordingFlow<?, ?> flow) {
    assertEquals(List.of(), flow.results());
    assertEquals("the failure was downgraded to a retryable error", List.of(), flow.errors());
    assertEquals(1, flow.permanentErrors().size());
    assertTrue(
        flow.permanentErrors().getFirst(),
        flow.permanentErrors().getFirst().contains(Integer.toString(REFUSED)));
  }

  private static <State extends Record, OriginalState extends Record, Value>
      RecordingFlow<State, Value> run(
          OperationAction<State, OriginalState, Value> action, OriginalState originalState) {
    final var flow = new RecordingFlow<State, Value>();
    action.launch(originalState).launch(new TestOperation(), new TestTransactionManager(), flow);
    return flow;
  }

  /**
   * Put a layer between the refusal and the terminal flow.
   *
   * <p>Only the layers that leave the state type alone can be built this way; the two that wrap the
   * state build their chains for themselves.
   */
  private static RecordingFlow<TestState, ?> runRefusedThrough(
      Function<
              OperationAction<TestState, TestState, HttpResponse<String>>,
              OperationAction<TestState, TestState, ?>>
          layer) {
    return run(layer.apply(refused()), new TestState("workflow-run"));
  }

  /**
   * The bottom of the chain does not forward, it decides: a control flow with no notion of retrying
   * treats a permanent failure as an ordinary one, which fails the operation just the same.
   */
  @Test
  public void aFlowWithNoOpinionReportsAPermanentErrorAsAnOrdinaryOne() {
    final var reported = new ArrayList<String>();
    new OperationControlFlow<TestState, String>() {

      @Override
      public void cancel() {}

      @Override
      public void error(String error) {
        reported.add(error);
      }

      @Override
      public void next(String result) {}

      @Override
      public tools.jackson.databind.JsonNode serializeNestedState(TestState state) {
        throw new UnsupportedOperationException();
      }
    }.permanentError("the credentials are wrong");
    assertEquals(List.of("the credentials are wrong"), reported);
  }

  @Test
  public void aBranchForwardsAPermanentError() {
    assertStillPermanent(
        run(
            OperationAction.branch(
                Map.<String, OperationAction<?, ?, HttpResponse<String>>>of("only", refused())),
            new BranchState(
                "only", OperationAction.MAPPER.valueToTree(new TestState("workflow-run")))));
  }

  @Test
  public void aReloadForwardsAPermanentError() {
    assertStillPermanent(runRefusedThrough(action -> action.reload(TestState::value)));
  }

  @Test
  public void aStatefulDebugInfoForwardsAPermanentError() {
    assertStillPermanent(
        runRefusedThrough(action -> action.then(OperationStatefulStep.debugInfo(neverCalled()))));
  }

  @Test
  public void aStatefulLogForwardsAPermanentError() {
    assertStillPermanent(
        runRefusedThrough(
            action -> action.then(OperationStatefulStep.log(Level.INFO, neverCalled()))));
  }

  @Test
  public void aStatefulMappingForwardsAPermanentError() {
    assertStillPermanent(
        runRefusedThrough(action -> action.then(OperationStatefulStep.mapping(neverCalled()))));
  }

  @Test
  public void aStatefulRequireForwardsAPermanentError() {
    assertStillPermanent(
        runRefusedThrough(
            action ->
                action.then(
                    OperationStatefulStep.require(
                        (state, value) -> {
                          throw new AssertionError("the check must not run after a refusal");
                        },
                        "the response was not acceptable"))));
  }

  @Test
  public void aStatefulStatusForwardsAPermanentError() {
    assertStillPermanent(
        runRefusedThrough(action -> action.then(OperationStatefulStep.status(neverCalled()))));
  }

  @Test
  public void aStepChainForwardsAPermanentError() {
    assertStillPermanent(
        run(
            OperationAction.load(TestState.class, TestState::value)
                .then(
                    OperationStep.<String, HttpResponse<String>>mapping(
                            ignored -> response(REFUSED, Map.of()))
                        .then(
                            new OperationStepHandleHttpStatus<String>()
                                .then(OperationStep.mapping(neverCalledStep())))),
            new TestState("workflow-run")));
  }

  @Test
  public void aStepFollowingAnActionForwardsAPermanentError() {
    assertStillPermanent(
        runRefusedThrough(action -> action.then(OperationStep.mapping(neverCalledStep()))));
  }

  /**
   * A poll cannot follow an HTTP status check directly, because it consumes a {@link PollResult},
   * so this covers the mapping that converts one into the other as well.
   */
  @Test
  public void aPollForwardsAPermanentError() {
    assertStillPermanent(
        runRefusedThrough(
            action ->
                action
                    .then(
                        OperationStep.<HttpResponse<String>, PollResult>mapping(neverCalledStep()))
                    .then(OperationStatefulStep.poll(DELAY))));
  }

  /**
   * The point of the whole exercise: the retry has to be told, or it starts the cycle that the
   * permanent error exists to prevent.
   */
  @Test
  public void aRetryForwardsAPermanentErrorWithoutRetrying() {
    final var transactionManager = new TestTransactionManager();
    final var flow =
        new RecordingFlow<RepeatCounter<TestState>, HttpResponse<String>>();
    refused()
        .then(OperationStatefulStep.repeatUntilSuccess(DELAY, 3))
        .launch(new TestState("workflow-run"))
        .launch(new TestOperation(), transactionManager, flow);
    assertStillPermanent(flow);
    assertEquals(List.of(), transactionManager.delays());
  }

  /**
   * The reason the retry forwards rather than reports, stated in its own comment: an enclosing
   * repeat must not start the futile cycle that the inner one has just declined to start.
   *
   * <p>Nesting also doubly wraps the state, so this is the only case where a {@link RepeatCounter}
   * has to serialize inside another one.
   */
  @Test
  public void aNestedRetryForwardsAPermanentErrorWithoutRetrying() {
    final var transactionManager = new TestTransactionManager();
    final var flow =
        new RecordingFlow<RepeatCounter<RepeatCounter<TestState>>, HttpResponse<String>>();
    refused()
        .then(OperationStatefulStep.repeatUntilSuccess(DELAY, 3))
        .then(OperationStatefulStep.repeatUntilSuccess(DELAY, 3))
        .launch(new TestState("workflow-run"))
        .launch(new TestOperation(), transactionManager, flow);
    assertStillPermanent(flow);
    assertEquals(
        "neither repeat should have spent an attempt", List.of(), transactionManager.delays());
  }

  /** A sub-step has to forward what the steps before it reported. */
  @Test
  public void aSubStepForwardsAPermanentErrorFromAboveIt() {
    assertStillPermanent(
        run(
            refused()
                .then(
                    OperationStatefulStep.subStep(
                        neverCalled(), OperationAction.load(SubState.class, SubState::value))),
            new TestState("workflow-run")));
  }

  /** It also has to forward what its child reported, which is a separate control flow. */
  @Test
  public void aSubStepForwardsAPermanentErrorFromItsChild() {
    assertStillPermanent(
        run(
            OperationAction.load(TestState.class, TestState::value)
                .then(
                    OperationStatefulStep.subStep(
                        (state, input) -> new SubState(input),
                        OperationAction.load(
                                SubState.class, ignored -> response(REFUSED, Map.of()))
                            .then(new OperationStepHandleHttpStatus<>()))),
            new TestState("workflow-run")));
  }
}
