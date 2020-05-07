package ca.on.oicr.gsi.vidarr.server.dto;

/** How a request to submit a job should be handled */
public enum SubmitMode {
  /**
   * Validate the submission and, if a match is found, return it. Find an appropriate workflow
   * engine to run it and have that workflow engine perform as much work as it can (e.g., check for
   * available resources, validate the workflow is compatible with the provided input).
   */
  DRY_RUN,
  /** Validate and run the workflow. */
  RUN,
  /**
   * Validate the submission, and if a match is found, return it. This does not check that the
   * workflow necessarily could schedule. It only checks that the input is well-formed and that
   * Víðarr would be willing to pass it to a workflow engine.
   */
  VALIDATE
}
