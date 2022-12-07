package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Control flow for executing {@link OperationAction}
 *
 * @param <State> the type of the operation's state
 * @param <Result> the type the operation should yield
 */
public interface OperationControlFlow<State, Result> {

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
