package ca.on.oicr.gsi.vidarr.core;

/** The steps (phases) for running a workflow */
public enum Phase {
  WAITING_FOR_RESOURCES,
  INITIALIZING,
  PREFLIGHT,
  PROVISION_IN,
  RUNNING,
  PROVISION_OUT,
  FAILED,
  CLEANUP;
}
