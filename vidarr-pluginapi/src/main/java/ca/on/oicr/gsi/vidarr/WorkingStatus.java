package ca.on.oicr.gsi.vidarr;

/** The status, as reported to the user, about a task */
public enum WorkingStatus {
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
