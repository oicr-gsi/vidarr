package ca.on.oicr.gsi.vidarr;

import java.io.PrintWriter;
import java.io.StringWriter;
import tools.jackson.databind.JsonNode;

/**
 * Control flow for executing {@link OperationAction}
 *
 * @param <State> the type of the operation's state
 * @param <Result> the type the operation should yield
 */
public interface OperationControlFlow<State, Result> {

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
    final var message = throwable.getMessage();
    return message == null || message.isBlank() ? throwable.toString() : message;
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
    }
  }

  /**
   * Return a successful value from an operation
   *
   * @param result the output value
   */
  void next(Result result);

  /**
   * Convert the state to JSON, performing and wrapping required
   *
   * @param state the state object to serialize
   * @return the JSON-encoded form of the state object and any necessary enclosing state
   */
  JsonNode serializeNestedState(State state);
}
