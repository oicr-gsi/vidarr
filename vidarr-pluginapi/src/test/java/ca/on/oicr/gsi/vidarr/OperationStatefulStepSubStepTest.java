package ca.on.oicr.gsi.vidarr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationStatefulStep.Child;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.util.List;
import org.junit.Test;

/**
 * A sub-step check-points its state and then resumes on Vidarr's executor
 *
 * <p>That makes it the step with the most places for a failure to go missing: spawning the child
 * state, writing the checkpoint, and re-entering itself to run the child all happen where nothing
 * observes the outcome. Each one has to end as a resolved operation.
 */
public class OperationStatefulStepSubStepTest {

  /** The child state; like the parent state, only the fact that it round-trips matters. */
  public record SubState(String value) {}

  private RecordingFlow<Child<TestState, SubState>, String> run(
      TestOperation operation,
      OperationStatefulStep.StatefulTransformer<TestState, String, SubState> spawn) {
    final var flow = new RecordingFlow<Child<TestState, SubState>, String>();
    OperationAction.load(TestState.class, TestState::value)
        .then(
            OperationStatefulStep.subStep(
                spawn, OperationAction.load(SubState.class, s -> s.value() + " provisioned")))
        .launch(new TestState("aligned.bam"))
        .launch(operation, new TestTransactionManager(), flow);
    return flow;
  }

  @Test
  public void theSubStepRunsAndItsResultComesBack() {
    final var operation = new TestOperation();
    final var flow = run(operation, (state, input) -> new SubState(input));
    assertEquals(List.of("aligned.bam provisioned"), flow.results());
    assertEquals(List.of(), flow.failures());
    // The checkpoint is what lets a restarted server resume inside the child rather than repeat it.
    assertEquals("aligned.bam", operation.recoveryState().get("child").get("value").asString());
  }

  /** Spawning the child runs plugin code, so it fails the operation rather than the thread. */
  @Test
  public void aSpawnThatThrowsFailsTheOperation() {
    final var flow =
        run(
            new TestOperation(),
            (state, input) -> {
              throw new IllegalArgumentException("no output directory was configured");
            });
    assertEquals(List.of(), flow.results());
    assertEquals(1, flow.errors().size());
    assertTrue(
        flow.errors().get(0), flow.errors().get(0).contains("no output directory was configured"));
  }

  /** The case that used to record a null error and leave nothing to diagnose. */
  @Test
  public void aSpawnThatThrowsWithoutAMessageStillReportsSomething() {
    final var flow =
        run(
            new TestOperation(),
            (state, input) -> {
              throw new NullPointerException();
            });
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("NullPointerException"));
  }

  /**
   * Writing the checkpoint happens on the executor, after the sub-step has already committed to
   * running, so a state that cannot be written would otherwise strand the run silently.
   */
  @Test
  public void aCheckpointThatCannotBeWrittenFailsTheOperation() {
    final var flow = new RecordingFlow<Child<TestState, SubState>, String>();
    flow.failOnSerialize(
        () -> {
          throw new UnsupportedOperationException("this state cannot be serialized");
        });
    OperationAction.load(TestState.class, TestState::value)
        .then(
            OperationStatefulStep.subStep(
                (state, input) -> new SubState(input),
                OperationAction.load(SubState.class, SubState::value)))
        .launch(new TestState("aligned.bam"))
        .launch(new TestOperation(), new TestTransactionManager(), flow);
    assertEquals(List.of(), flow.results());
    assertEquals(1, flow.errors().size());
    assertTrue(
        flow.errors().get(0), flow.errors().get(0).contains("this state cannot be serialized"));
    // A guarded failure reports the stack trace, because nothing anticipated this one.
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("OperationStatefulStepSubStep"));
  }

  /**
   * Whatever consumes the sub-step's output runs inside the task that resumed it, so a bug there
   * lands on the executor thread with nobody watching.
   */
  @Test
  public void aBugDownstreamOfTheSubStepFailsTheOperation() {
    final var flow = new RecordingFlow<Child<TestState, SubState>, String>();
    flow.failOnNext(
        () -> {
          throw new NullPointerException();
        });
    OperationAction.load(TestState.class, TestState::value)
        .then(
            OperationStatefulStep.subStep(
                (state, input) -> new SubState(input),
                OperationAction.load(SubState.class, SubState::value)))
        .launch(new TestState("aligned.bam"))
        .launch(new TestOperation(), new TestTransactionManager(), flow);
    assertEquals(1, flow.results().size());
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("NullPointerException"));
  }

  /**
   * A workflow run can be cancelled while the sub-step is being set up. That must unwind as a
   * cancellation, not as a failure and not as a child that runs anyway.
   */
  @Test
  public void anOperationCancelledWhileSpawningDoesNotRunTheChild() {
    final var operation = new TestOperation();
    final var flow =
        run(
            operation,
            (state, input) -> {
              // Stands in for an administrator cancelling the run at exactly this moment.
              operation.kill();
              return new SubState(input);
            });
    assertEquals(1, flow.cancels());
    assertEquals(List.of(), flow.results());
    assertEquals(List.of(), flow.failures());
    assertFalse(operation.isLive());
  }
}
