package ca.on.oicr.gsi.vidarr.core;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A running operation that can be updated by the scheduler
 *
 * @param <TX> the transaction type associated with this implementation
 */
public interface ActiveOperation<TX> {
  /**
   * Gets the last set debugging information for this operation
   *
   * @return the debugging information or a null JSON node
   */
  JsonNode debugInfo();

  /**
   * Change the current client-visible debugging information
   *
   * @param info the debugging information to store
   * @param transaction the transaction to perform the update in
   */
  void debugInfo(JsonNode info, TX transaction);

  /**
   * Check if the operation is still live
   *
   * @return if false, no callbacks will be scheduled.
   */
  boolean isLive();
  /**
   * Be told something something interesting
   *
   * @param level how important this message is
   * @param message the message to display
   */
  void log(System.Logger.Level level, String message);

  /** Get the current recovery state */
  JsonNode recoveryState();

  /**
   * Change the recovery state
   *
   * @param state the new recovery state
   * @param transaction the transaction to perform the update in
   */
  void recoveryState(JsonNode state, TX transaction);

  /** Get the current status */
  OperationStatus status();
  /**
   * Change the current status for the operation
   *
   * @param status the new status
   * @param transaction the transaction to perform the update in
   */
  void status(OperationStatus status, TX transaction);

  /** The plugin type associated with this operation */
  String type();

  /**
   * Change the plugin type assocated with this operation
   *
   * @param type the new plugin type
   * @param transaction the transaction to perform the update in
   */
  void type(String type, TX transaction);
}
