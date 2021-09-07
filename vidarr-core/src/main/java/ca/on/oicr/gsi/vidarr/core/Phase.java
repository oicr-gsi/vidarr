package ca.on.oicr.gsi.vidarr.core;

/***
 * The steps (phases) for running a workflow.
 * Phases proceed from top to bottom from WAITING_FOR_RESOURCES to CLEANUP in successful operation.
 * Any phase can transition to FAILED.
 * There are no transitions out of FAILED, however an external manager like Shesmu may kill and retry a FAILED
 * workflow run.
 */
public enum Phase {
  WAITING_FOR_RESOURCES,
  INITIALIZING,
  PREFLIGHT,
  PROVISION_IN,
  RUNNING,
  PROVISION_OUT,
  CLEANUP,
  FAILED;
}
