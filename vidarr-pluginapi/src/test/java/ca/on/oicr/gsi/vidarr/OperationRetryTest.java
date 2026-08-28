package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationTestDoubles.response;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationStatefulStep.RepeatCounter;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * What a step does about a failure has to survive the whole chain between it and the retry
 *
 * <p>The changelog promises that a busy Cromwell's 502 can be retried and that a refused request is
 * not worth retrying. Both claims are about {@link OperationStepHandleHttpStatus} and {@link
 * OperationStatefulStep#repeatUntilSuccess(Duration, int)} talking to each other through however
 * many intermediate control flows the action was built from, so test them together.
 */
public class OperationRetryTest {

  private static final int MAXIMUM_ATTEMPTS = 3;
  /** Long enough to be obvious in {@link TestTransactionManager#delays()}. */
  private static final Duration RETRY_DELAY = Duration.ofSeconds(7);

  /** How the run turned out, along with how many requests it took to get there. */
  private record Attempt(
      int requests, RecordingFlow<RepeatCounter<TestState>, HttpResponse<String>> flow) {}

  /**
   * Drive a request-and-check-the-status action wrapped in a retry, exactly as a plugin polling an
   * external service would build it.
   */
  private Attempt run(int status, TestTransactionManager transactionManager) {
    final var requests = new AtomicInteger();
    final var flow = new RecordingFlow<RepeatCounter<TestState>, HttpResponse<String>>();
    OperationAction.load(TestState.class, TestState::value)
        .then(
            OperationStep.<String, HttpResponse<String>>mapping(
                ignored -> {
                  requests.incrementAndGet();
                  return response(status, java.util.Map.of());
                }))
        .then(new OperationStepHandleHttpStatus<String>())
        .then(OperationStatefulStep.repeatUntilSuccess(RETRY_DELAY, MAXIMUM_ATTEMPTS))
        .launch(new TestState("workflow-run"))
        .launch(new TestOperation(), transactionManager, flow);
    return new Attempt(requests.get(), flow);
  }

  private Attempt run(int status) {
    return run(status, new TestTransactionManager());
  }

  /** A 502 from an overloaded service is the case the retry exists for. */
  @Test
  public void aTransientFailureIsRetriedUntilTheBudgetRunsOut() {
    final var transactionManager = new TestTransactionManager();
    final var attempt = run(502, transactionManager);
    assertEquals(MAXIMUM_ATTEMPTS + 1, attempt.requests());
    assertEquals(1, attempt.flow().errors().size());
    assertTrue(attempt.flow().errors().get(0), attempt.flow().errors().get(0).contains("502"));
    assertEquals(
        List.of(RETRY_DELAY.toSeconds(), RETRY_DELAY.toSeconds(), RETRY_DELAY.toSeconds()),
        transactionManager.delays());
  }

  /**
   * The point of the change: a refusal cannot be argued with, so the operation fails on the first
   * response instead of after the whole retry budget has been spent waiting.
   */
  @Test
  public void aPermanentFailureIsNotRetried() {
    for (final int status : new int[] {401, 403}) {
      final var transactionManager = new TestTransactionManager();
      final var attempt = run(status, transactionManager);
      assertEquals("status " + status, 1, attempt.requests());
      assertEquals("status " + status, List.of(), transactionManager.delays());
      assertEquals("status " + status, List.of(), attempt.flow().errors());
      assertEquals("status " + status, 1, attempt.flow().permanentErrors().size());
    }
  }

  /** A redirect is just as unarguable, and it reaches the retry through the same chain. */
  @Test
  public void aRedirectIsNotRetried() {
    final var attempt = run(301);
    assertEquals(1, attempt.requests());
    assertEquals(1, attempt.flow().permanentErrors().size());
  }

  @Test
  public void aSuccessIsNotRetried() {
    final var attempt = run(200);
    assertEquals(1, attempt.requests());
    assertEquals(1, attempt.flow().results().size());
    assertEquals(List.of(), attempt.flow().failures());
  }

  /**
   * A retry resumes on Vidarr's executor, so a bug in the retried work lands somewhere nothing
   * observes. It has to come back as an operation error rather than a run that never finishes.
   */
  @Test
  public void aBugInsideARetriedStepFailsTheOperation() {
    final var flow = new RecordingFlow<RepeatCounter<TestState>, String>();
    OperationAction.load(TestState.class, TestState::value)
        .then(
            OperationStep.<String, String>mapping(
                ignored -> {
                  throw new UnsupportedOperationException("this plugin is not finished");
                }))
        .then(OperationStatefulStep.repeatUntilSuccess(RETRY_DELAY, 1))
        .launch(new TestState("workflow-run"))
        .launch(new TestOperation(), new TestTransactionManager(), flow);
    // mapping catches its own transformer, so this is the ordinary error path rather than a guard.
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("this plugin is not finished"));
  }

  /** Sleeping hands control to the executor, so the work after it needs the same protection. */
  @Test
  public void aBugAfterASleepFailsTheOperation() {
    final var flow = new RecordingFlow<TestState, String>();
    final var transactionManager = new TestTransactionManager();
    flow.failOnNext(
        () -> {
          throw new NullPointerException();
        });
    OperationStep.<String>sleep(Duration.ofSeconds(5))
        .run("value", new TestOperation(), transactionManager, flow);
    assertEquals(List.of(5L), transactionManager.delays());
    assertEquals(1, flow.results().size());
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("NullPointerException"));
  }
}
