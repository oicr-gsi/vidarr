package ca.on.oicr.gsi.vidarr.server.dto;

/** The status of a workflow run */
public enum WorkflowStatus {
  /** The run is waiting for a consumable resource managed by Víðarr to become available */
  WAITING_ON_CONSUMABLE_RESOURCES,
  /** The run is waiting for a resource managed by the workflow engine to become available */
  WAITING_ON_WORKFLOW_ENGINE,
  /** The run is in some queue managed by the workflow engine */
  QUEUED,
  /** The run is in the workflow engine, but not in a meaningful state yet */
  SUBMITTED,
  /** The job is actively executing */
  RUNNING,
  /** The job has run successfully and now the output is being provisioned */
  PROVISIONING,
  /** The job failed while running */
  FAILED_RUNNING,
  /** The job failed while provisioning */
  FAILED_PROVISIONING,
  /** The job was determined to be invalid by Víðarr before execution */
  FAILED_VALIDATION,
  /** The job was determined to be invlid by the workflow engine before execution */
  FAILED_WORKFLOW_PRECHECK,
  /** The job has succeeded and the output has been provisioned */
  SUCCEEDED
}
