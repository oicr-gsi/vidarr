package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.TimeUnit;

/**
 * A mechanism to inform the Vidarr server about the state of a task
 *
 * <p>When Vidarr asks a plugin to launch a job for running or provisioning, it provides an object
 * for the plugin to use to manage the state of that record. Vidarr does not impose a particular
 * scheduling or process behaviour on plugins. Plugins may manage their tasks however they please
 * and asynchronously report their status back to Vidarr using a monitor.
 *
 * complete() and permanentFailure() are not to be called directly, but rather inside the Runnable parameter of
 * a call to scheduleTask(). This is to prevent database corruption - Vidarr needs to wait for database
 * transactions to complete before doing any scheduled tasks, including marking tasks as completed or failed.
 * Calling these methods directly would risk trying to write during or after that transaction rolls back in an
 * error state.
 * See vidarr-core BaseProcessor.java's startNextPhase(), BaseProcessor's BaseOperationMonitor,
 * and vidarr-server DatabaseWorkflow's phase()
 *
 * @param <T> the output type expected from the task
 * @param <S> the format state information is stored in
 */
public interface WorkMonitor<T, S> {

  /** The status, as reported to the user, about a task */
  enum Status {
    /**
     * The task's state cannot be accurately determined
     *
     * <p>This may indicate a remote system is not responding.
     */
    UNKNOWN,
    /** The task is not executing because it is waiting for resources to become available */
    WAITING,
    /** The task has been scheduled for execution */
    QUEUED,
    /** The task is currently executing */
    RUNNING,
    /** The task has been requested to stop executing for load reasons */
    THROTTLED
  }

  /**
   * The task is complete
   *
   * Do not call directly from plugin. Call inside the Runnable parameter of {@link #scheduleTask(Runnable)}
   * This is to prevent database corruption.
   *
   * <p>This can only be called once and cannot be called if {@link #permanentFailure(String)} has
   * been called.
   *
   * @param result the output data from the task
   */
  void complete(T result);

  /**
   * Write something interesting
   *
   * @param level how important this message is
   * @param message the message to display
   */
  void log(System.Logger.Level level, String message);

  /**
   * Indicate that the task is unrecoverably broken
   *
   * Do not call directly from plugin. Call inside the Runnable parameter of {@link #scheduleTask(Runnable)}
   * This is to prevent database corruption.
   *
   * <p>Once called, the workflow and related provisioning steps will be considered a failure.
   *
   * <p>This cannot be called after {@link #complete(Object)}
   *
   * @param reason the reason for the failure
   */
  void permanentFailure(String reason);

  /**
   * Request that Vidarr schedule a callback at the next available opportunity
   *
   * <p>This cannot be called after {@link #complete(Object)} or {@link #permanentFailure(String)}
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
   * <p>This cannot be called after {@link #complete(Object)} or {@link #permanentFailure(String)}
   *
   * @param delay the amount of time to wait; if delay is < 1, this is equivalent to {@link
   *     #scheduleTask(Runnable)}
   * @param units the time units to wait
   * @param task the task to execute
   */
  void scheduleTask(long delay, TimeUnit units, Runnable task);

  /**
   * Write debugging status information that will be made available to clients
   *
   * @param information the current information to be presented
   */
  void storeDebugInfo(JsonNode information);

  /**
   * Write the recovery information to stable store
   *
   * <p>If Vidarr is shutdown or crashes during execution, it will attempt to recover all running
   * tasks using information stored in its database. This updates this data for this task in the
   * case of crash recovery. The plugin is responsible for interpreting this data and being able to
   * recover state from other versions of the plugin in the case of upgrade.
   *
   * <p>This cannot be called after {@link #complete(Object)} or {@link #permanentFailure(String)}
   *
   * @param state the data to store
   */
  void storeRecoveryInformation(S state);

  /**
   * Update the state of the task
   *
   * <p>Vidarr uses this state information to provide more detailed information to the user about
   * the health of a task. It does not affect decision-making.
   *
   * <p>This cannot be called after {@link #complete(Object)} or {@link #permanentFailure(String)}
   *
   * @param status the new status of the task
   */
  void updateState(Status status);
}
