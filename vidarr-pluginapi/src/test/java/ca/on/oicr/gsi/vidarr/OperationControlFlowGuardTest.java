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
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
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

    private final List<String> errors = new ArrayList<>();

    @Override
    public void cancel() {}

    @Override
    public void error(String error) {
      // Record before throwing, so a test can tell a failure that was reported and rejected from
      // one that was never reported at all.
      errors.add(error);
      throw new IllegalStateException("Operation is already complete.");
    }

    List<String> errors() {
      return errors;
    }

    @Override
    public void next(String result) {}

    @Override
    public tools.jackson.databind.JsonNode serializeNestedState(TestState state) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A failure whose cause is itself.
   *
   * <p>{@link Throwable#initCause(Throwable)} rejects this, but {@link Throwable#getCause()} is
   * overridable, so an exception class that computes its cause can still produce it.
   */
  private static final class SelfCausedException extends RuntimeException {

    private SelfCausedException(String message) {
      super(message);
    }

    @Override
    public Throwable getCause() {
      return this;
    }
  }

  /** A future's wrapper whose cause is itself, which the unwrapping walk must not follow. */
  private static final class SelfCausedCompletionException extends CompletionException {

    private SelfCausedCompletionException(String message) {
      super(message);
    }

    @Override
    public Throwable getCause() {
      return this;
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
    assertTrue(error, error.startsWith("Unhandled exception while running operation: "));
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
    final var flow = new BrokenFlow();
    flow.guard(
        () -> {
          throw new IllegalArgumentException("boom");
        });
    assertEquals(1, flow.errors().size());
    final var error = flow.errors().get(0);
    assertTrue(error, error.contains("boom"));
    assertTrue(error, error.contains("OperationControlFlowGuardTest"));
  }

  @Test
  public void successfulWorkIsUntouched() {
    final var flow = new RecordingFlow<TestState, String>();
    flow.guard(() -> flow.next("fine"));
    assertEquals(1, flow.results().size());
    assertEquals(List.of(), flow.failures());
    assertEquals(0, flow.cancels());
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
   * fall back to naming the exception rather than report the blank.
   *
   * <p>The fallback is {@code toString()}, which appends any non-null message however blank it is,
   * so a blank message is still reported as a trailing colon and whatever whitespace it held. That
   * is not identical to the missing-message case, but it names the exception, which is the point.
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

  /**
   * A cause chain is not guaranteed to terminate, and the code that stops workflow runs from
   * hanging must not hang on one, so the walk gives up at {@link
   * OperationControlFlow#MAXIMUM_CAUSE_DEPTH}.
   *
   * <p>Giving up means reporting a wrapper, which is worse than the exception underneath but is
   * still enough to work from, and nothing legitimate nests this deeply anyway.
   */
  @Test
  public void describeStopsAtTheCauseDepthLimit() {
    Throwable failure = new ConnectException("connection refused");
    for (var depth = 0; depth < OperationControlFlow.MAXIMUM_CAUSE_DEPTH + 4; depth++) {
      failure = new CompletionException(failure);
    }
    final var described = OperationControlFlow.describe(failure);
    assertTrue(described, described.contains("CompletionException"));
    // Each wrapper's message is its cause's toString(), so the real problem is buried, not lost.
    assertTrue(described, described.contains("connection refused"));
  }

  /**
   * The limit is the number of wrappers the walk will strip, so a chain exactly that long still
   * unwraps completely.
   */
  @Test
  public void describeUnwrapsAChainAtTheDepthLimit() {
    Throwable failure = new ConnectException("connection refused");
    for (var depth = 0; depth < OperationControlFlow.MAXIMUM_CAUSE_DEPTH; depth++) {
      failure = new CompletionException(failure);
    }
    assertEquals("connection refused", OperationControlFlow.describe(failure));
  }

  /** A wrapper that is its own cause has nothing underneath to report, so it reports itself. */
  @Test
  public void describeStopsAtASelfReferentialCause() {
    assertEquals(
        "stuck", OperationControlFlow.describe(new SelfCausedCompletionException("stuck")));
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
      assertTrue(flow.errors().get(0), flow.errors().get(0).contains("InterruptedException"));
    } finally {
      // Do not leak the flag into whatever test runs next on this thread.
      Thread.interrupted();
    }
  }

  /**
   * An interrupt reaches a step as a channel closing under it just as readily as as an {@link
   * InterruptedException}, and it clears the flag the same way.
   */
  @Test
  public void aChannelClosedByAnInterruptRestoresTheFlag() {
    final var flow = new RecordingFlow<TestState, String>();
    try {
      flow.guard(
          () -> {
            throw new CompletionException(new ClosedByInterruptException());
          });
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(1, flow.errors().size());
    } finally {
      // Do not leak the flag into whatever test runs next on this thread.
      Thread.interrupted();
    }
  }

  /**
   * Looking for an interrupt walks the same untrustworthy cause chain, so a self-referential one
   * must be reported rather than followed forever.
   */
  @Test
  public void aSelfReferentialCauseChainIsReportedRatherThanWalked() {
    final var flow = new RecordingFlow<TestState, String>();
    flow.guard(
        () -> {
          throw new SelfCausedException("its own cause");
        });
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("its own cause"));
    assertFalse(Thread.currentThread().isInterrupted());
  }

  /**
   * The depth limit is a trade: an interrupt buried deeper than the walk goes is missed, so the
   * flag is not restored. The failure is still reported, which is what stops the run from hanging,
   * and nothing legitimate wraps an interrupt this deeply.
   */
  @Test
  public void anInterruptBuriedPastTheDepthLimitIsStillReported() {
    final var flow = new RecordingFlow<TestState, String>();
    Throwable buried = new InterruptedException();
    for (var depth = 0; depth < OperationControlFlow.MAXIMUM_CAUSE_DEPTH + 4; depth++) {
      buried = new CompletionException(buried);
    }
    final var failure = buried;
    try {
      flow.guard(
          () -> {
            throw new CompletionException(failure);
          });
      assertEquals(1, flow.errors().size());
      assertFalse(Thread.currentThread().isInterrupted());
    } finally {
      // If the walk ever does reach this deep, do not leak the flag into the next test.
      Thread.interrupted();
    }
  }

  /** An ordinary failure must not leave the thread looking like it was asked to stop. */
  @Test
  public void anOrdinaryFailureLeavesTheInterruptFlagAlone() {
    final var flow = new RecordingFlow<TestState, String>();
    // Whatever ran before on this thread decides the starting state, so do not inherit it.
    Thread.interrupted();
    flow.guard(
        () -> {
          throw new IllegalStateException("nothing to do with interrupts");
        });
    assertFalse(Thread.currentThread().isInterrupted());
    assertEquals(1, flow.errors().size());
  }

  /**
   * The other half of leaving the flag alone: a thread that was already asked to stop must still
   * look that way after an unrelated failure, or the shutdown it was told about is lost.
   */
  @Test
  public void anOrdinaryFailureDoesNotClearAnInterruptThatWasAlreadySet() {
    final var flow = new RecordingFlow<TestState, String>();
    try {
      Thread.currentThread().interrupt();
      flow.guard(
          () -> {
            throw new IllegalStateException("nothing to do with interrupts");
          });
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(1, flow.errors().size());
    } finally {
      // Do not leak the flag into whatever test runs next on this thread.
      Thread.interrupted();
    }
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
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("StackOverflowError"));
  }
}
