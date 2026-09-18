package ca.on.oicr.gsi.vidarr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import org.junit.Test;

/**
 * Polling waits for an external service, which is what a stalled run looks like from the outside
 *
 * <p>Each round of a poll is scheduled on Vidarr's executor, so the work that decides whether to
 * keep waiting runs where nothing observes its outcome. A bug there has to fail the operation
 * instead of quietly ending the polling and leaving the workflow run waiting for a service that was
 * never going to be asked again.
 */
public class OperationStatefulStepPollTest {

  /** Long enough to be obvious in {@link TestTransactionManager#delays()}. */
  private static final Duration DELAY = Duration.ofSeconds(11);

  /** Poll until the supplier says to stop, as a plugin watching a workflow engine would. */
  private RecordingFlow<TestState, Void> run(
      TestOperation operation,
      TestTransactionManager transactionManager,
      RecordingFlow<TestState, Void> flow,
      Supplier<PollResult> results) {
    OperationAction.load(TestState.class, state -> results.get())
        .then(OperationStatefulStep.<TestState, TestState>poll(DELAY))
        .launch(new TestState("workflow-run"))
        .launch(operation, transactionManager, flow);
    return flow;
  }

  /** A poll that returns each result in turn, so a test can say how the waiting ends. */
  private Supplier<PollResult> sequence(PollResult... results) {
    final var remaining = List.of(results).iterator();
    return () -> remaining.next();
  }

  @Test
  public void aFinishedPollCompletesTheStep() {
    final var transactionManager = new TestTransactionManager();
    final var flow =
        run(
            new TestOperation(),
            transactionManager,
            new RecordingFlow<>(),
            sequence(PollResult.finished()));
    assertEquals(1, flow.results().size());
    assertEquals(List.of(), flow.failures());
    assertEquals("nothing should have been rescheduled", List.of(), transactionManager.delays());
  }

  @Test
  public void anActivePollIsRescheduledUntilItFinishes() {
    final var operation = new TestOperation();
    final var transactionManager = new TestTransactionManager();
    final var flow =
        run(
            operation,
            transactionManager,
            new RecordingFlow<>(),
            sequence(PollResult.active(WorkingStatus.RUNNING), PollResult.finished()));
    assertEquals(List.of(DELAY.toSeconds()), transactionManager.delays());
    assertEquals(1, flow.results().size());
    assertEquals(List.of(), flow.failures());
    // The status is how a waiting run explains itself to whoever is watching it.
    assertEquals(OperationStatus.of(WorkingStatus.RUNNING), operation.status());
  }

  @Test
  public void aFailedPollFailsTheOperation() {
    final var flow =
        run(
            new TestOperation(),
            new TestTransactionManager(),
            new RecordingFlow<>(),
            sequence(PollResult.failed("Cromwell gave up on this workflow")));
    assertEquals(List.of(), flow.results());
    assertEquals(List.of("Cromwell gave up on this workflow"), flow.errors());
  }

  /** A run cancelled while a poll is outstanding must unwind rather than finish the step. */
  @Test
  public void aCancelledOperationStopsPolling() {
    final var operation = new TestOperation();
    final var transactionManager = new TestTransactionManager();
    final var flow =
        run(
            operation,
            transactionManager,
            new RecordingFlow<>(),
            () -> {
              // Stands in for an administrator cancelling the run while Vidarr was waiting.
              operation.kill();
              return PollResult.finished();
            });
    assertEquals(1, flow.cancels());
    assertEquals(List.of(), flow.results());
    assertEquals(List.of(), flow.failures());
  }

  /**
   * The regression: the round that resumes on the executor is the one nothing is watching, so a bug
   * in it has to come back as a failed operation.
   */
  @Test
  public void aBugInARescheduledPollFailsTheOperation() {
    final var flow = new RecordingFlow<TestState, Void>();
    flow.failOnNext(
        () -> {
          throw new NullPointerException();
        });
    run(
        new TestOperation(),
        new TestTransactionManager(),
        flow,
        sequence(PollResult.active(WorkingStatus.RUNNING), PollResult.finished()));
    assertEquals(1, flow.results().size());
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("NullPointerException"));
    // A guarded failure reports the stack trace, because nothing anticipated this one.
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("OperationStatefulStepPoll"));
  }
}
