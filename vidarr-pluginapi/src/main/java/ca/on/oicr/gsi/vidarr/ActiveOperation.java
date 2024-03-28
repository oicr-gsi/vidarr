package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A running operation that can be updated by the scheduler
 *
 * @param <TX> the transaction type associated with this implementation
 */
public interface ActiveOperation<TX> {

  /**
   * The database where transaction operations can be serialized
   *
   * @param <TX> the transaction type
   */
  interface TransactionManager<TX> {

    /**
     * Performs an operation in a transaction.
     *
     * @param transaction a callback to perform in a transaction; not that the transaction object
     *     should not outlive this method call
     */
    void inTransaction(Consumer<TX> transaction);

    /**
     * Request that Vidarr schedule a callback at the next available opportunity
     *
     * @param task the task to execute
     */
    void scheduleTask(Runnable task);

    /**
     * Request that Vidarr schedule a callback at a specified time in the future
     *
     * <p>This scheduling is best-effort; Vidarr may execute a task earlier or later than requested
     * based on load and priority.
     *
     * @param delay the amount of time to wait; if delay is < 1, this is equivalent to {@link
     *     #scheduleTask(Runnable)}
     * @param units the time units to wait
     * @param task the task to execute
     */
    void scheduleTask(long delay, TimeUnit units, Runnable task);
  }
  /**
   * Change the current client-visible debugging information
   *
   * @param info the debugging information to store
   * @param transaction the transaction to perform the update in
   */
  void debugInfo(JsonNode info, TX transaction);

  /**
   * Set the failure message
   *
   * @param reason the failure reason
   * @param transaction the transaction to perform the update in
   */
  void error(String reason, TX transaction);

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
   * Change the plugin type associated with this operation
   *
   * @param type the new plugin type
   * @param transaction the transaction to perform the update in
   */
  void type(String type, TX transaction);
}
