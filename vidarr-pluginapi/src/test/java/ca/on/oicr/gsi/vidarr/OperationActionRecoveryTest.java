package ca.on.oicr.gsi.vidarr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationAction.Launcher;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.util.List;
import java.util.function.Function;
import org.junit.Test;
import tools.jackson.databind.JsonNode;

/**
 * Resuming an operation from the database has to be as well guarded as starting one
 *
 * <p>{@code launch} runs its action inline, so the guard that catches a bug in it belongs to
 * whatever scheduled the launch; {@code TaskStarter} wraps that call. {@code recover} and {@code
 * retry} instead schedule the work themselves, which puts it on Vidarr's executor where nothing
 * observes the outcome, so each has to guard its own task.
 *
 * <p>That path runs when the server restarts with operations still in flight, which is when a
 * workflow run that never resolves is hardest to notice: there is no failure to look at and no
 * recent submission to connect it to.
 */
public class OperationActionRecoveryTest {

  /** The action being resumed; loading the state is enough to reach the successor that fails. */
  private static final OperationAction<TestState, TestState, String> ACTION =
      OperationAction.load(TestState.class, TestState::value);

  /** The state as it would have been written to the database before the server went down. */
  private static final JsonNode STATE = ACTION.launch(new TestState("workflow-run")).state();

  /** Drive a launcher whose successor throws, as a bug downstream of the resumed step would. */
  private static RecordingFlow<TestState, String> runWithABrokenSuccessor(
      Function<OperationAction<TestState, TestState, String>, Launcher<TestState, String>> start) {
    final var flow = new RecordingFlow<TestState, String>();
    flow.failOnNext(
        () -> {
          throw new NullPointerException();
        });
    start.apply(ACTION).launch(new TestOperation(), new TestTransactionManager(), flow);
    return flow;
  }

  private static void assertReportedAsUnhandled(RecordingFlow<TestState, String> flow) {
    assertEquals(1, flow.errors().size());
    final var error = flow.errors().getFirst();
    assertTrue(error, error.startsWith("Unhandled exception while running operation: "));
    assertTrue(error, error.contains("NullPointerException"));
  }

  @Test
  public void aBugAfterRecoveringFailsTheOperation() {
    final var flow = runWithABrokenSuccessor(action -> action.recover(STATE));
    assertEquals(1, flow.results().size());
    assertReportedAsUnhandled(flow);
  }

  @Test
  public void aBugAfterRetryingFailsTheOperation() {
    final var flow = runWithABrokenSuccessor(action -> action.retry(STATE));
    assertEquals(1, flow.results().size());
    assertReportedAsUnhandled(flow);
  }

  /** The guard must not get in the way of the resumption it is protecting. */
  @Test
  public void aRecoveredOperationResumesFromItsSavedState() {
    final var flow = new RecordingFlow<TestState, String>();
    ACTION.recover(STATE).launch(new TestOperation(), new TestTransactionManager(), flow);
    assertEquals(List.of("workflow-run"), flow.results());
    assertEquals(List.of(), flow.failures());
  }

  @Test
  public void aRetriedOperationResumesFromItsSavedState() {
    final var flow = new RecordingFlow<TestState, String>();
    ACTION.retry(STATE).launch(new TestOperation(), new TestTransactionManager(), flow);
    assertEquals(List.of("workflow-run"), flow.results());
    assertEquals(List.of(), flow.failures());
  }
}
