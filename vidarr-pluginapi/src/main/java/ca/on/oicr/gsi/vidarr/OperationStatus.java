package ca.on.oicr.gsi.vidarr;

/** The status for an operation */
public enum OperationStatus {
  INITIALIZING,
  PLUGIN_UNKNOWN,
  PLUGIN_WAITING,
  PLUGIN_QUEUED,
  PLUGIN_RUNNING,
  PLUGIN_THROTTLED,
  FAILED,
  SUCCEEDED;

  public static OperationStatus of(WorkingStatus status) {
    return switch (status) {
      case WAITING -> PLUGIN_WAITING;
      case QUEUED -> PLUGIN_QUEUED;
      case RUNNING -> PLUGIN_RUNNING;
      case THROTTLED -> PLUGIN_THROTTLED;
      default -> PLUGIN_UNKNOWN;
    };
  }
}
