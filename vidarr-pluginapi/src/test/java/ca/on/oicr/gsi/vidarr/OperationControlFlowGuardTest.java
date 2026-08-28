package ca.on.oicr.gsi.vidarr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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
    final var flow = new RecordingFlow<TestState, String>();
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
    final var flow = new RecordingFlow<TestState, String>();
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
    final var flow = new RecordingFlow<TestState, String>();
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
    final var flow = new RecordingFlow<TestState, String>();
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
    final var flow = new RecordingFlow<TestState, String>();
    OperationStep.<String>future()
        .run(
            CompletableFuture.failedFuture(new ConnectException()),
            new TestOperation(),
            new TestTransactionManager(),
            flow);
    assertEquals(1, flow.errors().size());
    assertNotNull(flow.errors().get(0));
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("ConnectException"));
  }

  @Test
  public void describeUsesTheMessageWhenThereIsOne() {
    assertEquals(
        "outputDirectory is missing",
        OperationControlFlow.describe(new IllegalArgumentException("outputDirectory is missing")));
  }

  /**
   * A blank message is as useless as a missing one, and some libraries throw with one, so it must
   * fall back the same way.
   */
  @Test
  public void describeFallsBackWhenTheMessageIsEmptyOrBlank() {
    assertEquals(
        "java.lang.IllegalStateException",
        OperationControlFlow.describe(new IllegalStateException()));
    assertEquals(
        "java.lang.IllegalStateException: ",
        OperationControlFlow.describe(new IllegalStateException("")));
    assertEquals(
        "java.lang.IllegalStateException:    ",
        OperationControlFlow.describe(new IllegalStateException("   ")));
  }

  /**
   * A step that chains onto a future gets the failure wrapped, and the wrapper's message is only
   * the cause's {@code toString()}, so reporting it buries the real problem in boilerplate.
   */
  @Test
  public void describeUnwrapsTheFutureMachinery() {
    assertEquals(
        "connection refused",
        OperationControlFlow.describe(
            new CompletionException(new ConnectException("connection refused"))));
    assertEquals(
        "connection refused",
        OperationControlFlow.describe(
            new ExecutionException(new ConnectException("connection refused"))));
    // Nested wrappers happen when a chained future fails inside another chained future.
    assertEquals(
        "connection refused",
        OperationControlFlow.describe(
            new CompletionException(
                new ExecutionException(new ConnectException("connection refused")))));
  }

  /** Unwrapping a cause that has no message must still not report a bare wrapper class name. */
  @Test
  public void describeUnwrapsToAMessagelessCause() {
    assertEquals(
        "java.net.ConnectException",
        OperationControlFlow.describe(new CompletionException(new ConnectException())));
  }

  /** A wrapper with nothing underneath is all there is to report. */
  @Test
  public void describeKeepsAWrapperWithNoCause() {
    assertEquals(
        "no cause", OperationControlFlow.describe(new ExecutionException("no cause", null)));
  }

  /** Anything that is not future machinery is reported as-is, cause chain and all. */
  @Test
  public void describeDoesNotUnwrapOrdinaryExceptions() {
    assertEquals(
        "could not read the manifest",
        OperationControlFlow.describe(
            new IllegalStateException(
                "could not read the manifest", new IOException("disk gone"))));
  }

  /**
   * Throwing an {@link InterruptedException} clears the interrupt flag, and Vidarr only interrupts
   * these threads to shut them down, so swallowing the request would be a shutdown that hangs.
   */
  @Test
  public void anInterruptIsReportedAndTheFlagIsRestored() {
    final var flow = new RecordingFlow<TestState, String>();
    assertFalse(Thread.currentThread().isInterrupted());
    try {
      flow.guard(
          () -> {
            throw new CompletionException(new InterruptedException());
          });
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(1, flow.errors().size());
    } finally {
      // Do not leak the flag into whatever test runs next on this thread.
      Thread.interrupted();
    }
  }

  /** An ordinary failure must not leave the thread looking like it was asked to stop. */
  @Test
  public void anOrdinaryFailureLeavesTheInterruptFlagAlone() {
    final var flow = new RecordingFlow<TestState, String>();
    flow.guard(
        () -> {
          throw new IllegalStateException("nothing to do with interrupts");
        });
    assertFalse(Thread.currentThread().isInterrupted());
  }

  /**
   * A broken JVM is not one workflow run's problem. The operation is still failed, so no run is
   * left waiting, but the error keeps travelling so the server does not quietly carry on.
   */
  @Test
  public void aFatalErrorIsReportedAndThenRethrown() {
    final var flow = new RecordingFlow<TestState, String>();
    final var thrown =
        assertThrows(
            OutOfMemoryError.class,
            () ->
                flow.guard(
                    () -> {
                      throw new OutOfMemoryError("Java heap space");
                    }));
    assertEquals("Java heap space", thrown.getMessage());
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("OutOfMemoryError"));
  }

  /**
   * A {@link StackOverflowError} is a runaway step rather than a broken JVM, and its stack has
   * already unwound by the time it is caught, so it must stay contained like any other bug.
   */
  @Test
  public void aStackOverflowIsContainedRatherThanRethrown() {
    final var flow = new RecordingFlow<TestState, String>();
    flow.guard(
        () -> {
          throw new StackOverflowError();
        });
    assertEquals(1, flow.errors().size());
  }
}
