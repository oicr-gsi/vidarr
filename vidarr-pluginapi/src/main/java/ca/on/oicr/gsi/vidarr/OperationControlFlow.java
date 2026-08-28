package ca.on.oicr.gsi.vidarr;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import tools.jackson.databind.JsonNode;

/**
 * Control flow for executing {@link OperationAction}
 *
 * @param <State> the type of the operation's state
 * @param <Result> the type the operation should yield
 */
public interface OperationControlFlow<State, Result> {

  /**
   * The number of causes that {@link #describe(Throwable)} and {@link #guard(Runnable)} will walk
   *
   * <p>Nothing legitimate nests this deeply; the limit exists only so that a malformed cause chain
   * cannot hang the code that is meant to stop workflow runs from hanging.
   */
  int MAXIMUM_CAUSE_DEPTH = 16;

  /**
   * Determine whether a failure was caused by this thread being interrupted
   *
   * @param throwable the failure to inspect
   * @return true if an interrupt appears anywhere in the cause chain
   */
  private static boolean causedByInterrupt(Throwable throwable) {
    var current = throwable;
    // A cause chain is not guaranteed to terminate, and looping forever in the code that exists to
    // stop a workflow run from hanging would be its own version of this bug, so bound the walk.
    for (var depth = 0; current != null && depth < MAXIMUM_CAUSE_DEPTH; depth++) {
      if (current instanceof InterruptedException
          || current instanceof ClosedByInterruptException) {
        return true;
      }
      final var cause = current.getCause();
      current = cause == current ? null : cause;
    }
    return false;
  }

  /**
   * Describe a failure that a step could reasonably expect to encounter
   *
   * <p>Many failures, notably {@link NullPointerException}, carry no message. Reporting {@code
   * getMessage()} directly means the operation records a null error and the reason for the failure
   * is lost, so always describe throwables through this method.
   *
   * @param throwable the failure to describe
   * @return a non-null description of the failure
   */
  static String describe(Throwable throwable) {
    final var reported = unwrapFutureFailure(throwable);
    final var message = reported.getMessage();
    return message == null || message.isBlank() ? reported.toString() : message;
  }

  /**
   * Describe a failure that indicates a bug rather than a problem with an external service
   *
   * <p>The stack trace is included because the failure was not anticipated by the step that threw
   * it, so the message alone is rarely enough to find the cause.
   *
   * @param throwable the failure to describe
   * @return a non-null description of the failure, including a stack trace
   */
  static String describeUnhandled(Throwable throwable) {
    final var buffer = new StringWriter();
    buffer.append("Unhandled exception while running operation: ");
    try (final var writer = new PrintWriter(buffer)) {
      throwable.printStackTrace(writer);
    }
    return buffer.toString();
  }

  /**
   * Determine whether a failure means the JVM itself is no longer usable
   *
   * <p>A {@link StackOverflowError} is a bug in the step that overflowed, and its stack has already
   * unwound by the time it is caught, so it is treated like any other bug. The remaining {@link
   * VirtualMachineError}s say that the JVM cannot continue, which is not something a single
   * workflow run should quietly absorb on the whole server's behalf.
   *
   * @param throwable the failure to inspect
   * @return true if the failure should be allowed to escape after it has been reported
   */
  private static boolean isFatal(Throwable throwable) {
    return throwable instanceof VirtualMachineError && !(throwable instanceof StackOverflowError);
  }

  /**
   * Strip the wrappers that {@link java.util.concurrent.CompletableFuture} puts around a failure
   *
   * <p>A future that a step chained onto another reports its failure as a {@link
   * CompletionException} whose own message is the cause's {@code toString()}, so describing the
   * wrapper buries the real problem in boilerplate, and a cause with no message leaves nothing but
   * the wrapper's class name. The exception worth reporting is the one underneath.
   *
   * @param throwable the failure to unwrap
   * @return the innermost failure that is not a future's wrapper
   */
  private static Throwable unwrapFutureFailure(Throwable throwable) {
    var current = throwable;
    // As in causedByInterrupt, the cause chain cannot be trusted to terminate.
    for (var depth = 0; depth < MAXIMUM_CAUSE_DEPTH; depth++) {
      if (!(current instanceof CompletionException || current instanceof ExecutionException)) {
        return current;
      }
      final var cause = current.getCause();
      if (cause == null || cause == current) {
        return current;
      }
      current = cause;
    }
    return current;
  }

  /**
   * Perform cleanup for an operation has been externally terminated
   *
   * <p>This should bubble up the call stack
   */
  void cancel();

  /**
   * Abort a running operation due to an error
   *
   * <p>Perform cleanup as necessary
   *
   * @param error the error message that should be reported
   */
  void error(String error);

  /**
   * Run a block of work, reporting any exception that escapes it as an operation error
   *
   * <p>Steps hand control to each other from HTTP completion callbacks and from tasks scheduled on
   * Vidarr's executor, and nothing observes the outcome of either. An exception that escapes one of
   * those is therefore invisible: the operation is never resolved, so the workflow run waits
   * forever for a callback that will never come. Any step that resumes work on another thread must
   * route that work through this method so that a bug becomes a failed operation with a stack trace
   * instead of a workflow run that hangs.
   *
   * @param task the work to perform
   */
  default void guard(Runnable task) {
    try {
      task.run();
    } catch (Throwable e) {
      if (causedByInterrupt(e)) {
        /* Throwing an InterruptedException clears the interrupt flag. Vidarr only interrupts these
         * threads when it is shutting the executor down, so restore the flag rather than leave the
         * shutdown looking like it has been dealt with. */
        Thread.currentThread().interrupt();
      }
      try {
        error(describeUnhandled(e));
      } catch (Throwable failure) {
        // There is nothing left to report the failure to, so make sure it is at least not silent.
        failure.addSuppressed(e);
        System.getLogger(OperationControlFlow.class.getName())
            .log(
                System.Logger.Level.ERROR,
                "Failed to report unhandled exception in operation",
                failure);
      }
      if (isFatal(e)) {
        /* The operation has been failed, so no workflow run is left waiting on it, which is all
         * this method promises. Recording a broken JVM as one workflow run's problem and carrying
         * on would hide it, so let it escape to the executor as well. */
        throw (Error) e;
      }
    }
  }

  /**
   * Return a successful value from an operation
   *
   * @param result the output value
   */
  void next(Result result);

  /**
   * Abort a running operation due to an error that reattempting cannot fix
   *
   * <p>{@link OperationStatefulStep#repeatUntilSuccess(java.time.Duration, int)} exists because
   * many failures are transient, but some are not: a request refused because Vidarr is not
   * authorised to make it will be refused identically however many times it is repeated, and
   * retrying only delays the report by the whole retry budget. A step that can tell the difference
   * should report those failures here so that they fail the operation immediately.
   *
   * <p>The default treats the failure as retryable, which is the right answer for a control flow
   * that has no notion of retrying. An implementation only needs to override this if it either
   * retries or sits between a step and something that does; forgetting to forward it costs nothing
   * worse than the retries that would have happened anyway.
   *
   * @param error the error message that should be reported
   */
  default void permanentError(String error) {
    error(error);
  }

  /**
   * Convert the state to JSON, performing and wrapping required
   *
   * @param state the state object to serialize
   * @return the JSON-encoded form of the state object and any necessary enclosing state
   */
  JsonNode serializeNestedState(State state);
}
