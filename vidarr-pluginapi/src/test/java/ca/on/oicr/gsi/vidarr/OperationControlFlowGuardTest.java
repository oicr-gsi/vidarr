package ca.on.oicr.gsi.vidarr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

/**
 * {@link OperationControlFlow#guard(Runnable)} is what stops a bug in a step, a plugin or a phase
 * transition from silently stalling a workflow run.
 */
public class OperationControlFlowGuardTest {

  /** A flow whose error handler is itself broken, as it is once the operation has been resolved. */
  private static final class BrokenFlow implements OperationControlFlow<TestState, String> {

    @Override
    public void cancel() {}

    @Override
    public void error(String error) {
      throw new IllegalStateException("Operation is already complete.");
    }

    @Override
    public void next(String result) {}

    @Override
    public tools.jackson.databind.JsonNode serializeNestedState(TestState state) {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void aFailureReportsTheMessageAndTheStackTrace() {
    final var flow = new RecordingFlow<String>();
    flow.guard(
        () -> {
          throw new IllegalArgumentException("outputDirectory is missing");
        });
    assertEquals(1, flow.errors().size());
    final var error = flow.errors().get(0);
    assertTrue(error, error.contains("outputDirectory is missing"));
    assertTrue(error, error.contains("OperationControlFlowGuardTest"));
  }

  /** A bare NPE has no message, which is exactly the case that used to leave nothing to go on. */
  @Test
  public void aFailureWithoutAMessageStillReportsSomethingUseful() {
    final var flow = new RecordingFlow<String>();
    flow.guard(
        () -> {
          final String nothing = null;
          nothing.length();
        });
    assertEquals(1, flow.errors().size());
    final var error = flow.errors().get(0);
    assertTrue(error, error.contains("NullPointerException"));
    assertTrue(error, error.contains("OperationControlFlowGuardTest"));
  }

  @Test
  public void anErrorIsAlsoCaught() {
    final var flow = new RecordingFlow<String>();
    flow.guard(
        () -> {
          throw new StackOverflowError();
        });
    assertEquals(1, flow.errors().size());
  }

  /** Reporting must never itself escape, or we are back to an exception nobody sees. */
  @Test
  public void aBrokenErrorHandlerDoesNotPropagate() {
    new BrokenFlow()
        .guard(
            () -> {
              throw new IllegalArgumentException("boom");
            });
  }

  @Test
  public void successfulWorkIsUntouched() {
    final var flow = new RecordingFlow<String>();
    flow.guard(() -> flow.next("fine"));
    assertEquals(1, flow.results().size());
    assertEquals(0, flow.errors().size());
  }

  /**
   * The whole rest of an operation runs inside the HTTP completion callback, so a bug anywhere
   * downstream of a request has to come back as an operation error.
   */
  @Test
  public void aFailureDownstreamOfAnHttpCallIsReported() {
    final var flow = new RecordingFlow<String>();
    flow.failOnNext(
        () -> {
          throw new NullPointerException();
        });
    OperationStep.<String>future()
        .run(
            CompletableFuture.completedFuture("body"),
            new TestOperation(),
            new TestTransactionManager(),
            flow);
    assertEquals(1, flow.results().size());
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0).contains("NullPointerException"));
  }

  /** A failed request must report something, even though its cause carries no message. */
  @Test
  public void aFailedHttpCallReportsANonNullError() {
    final var flow = new RecordingFlow<String>();
    OperationStep.<String>future()
        .run(
            CompletableFuture.failedFuture(new java.net.ConnectException()),
            new TestOperation(),
            new TestTransactionManager(),
            flow);
    assertEquals(1, flow.errors().size());
    assertNotNull(flow.errors().get(0));
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("ConnectException"));
  }
}
