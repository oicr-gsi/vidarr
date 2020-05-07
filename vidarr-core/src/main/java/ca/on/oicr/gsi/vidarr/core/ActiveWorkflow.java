package ca.on.oicr.gsi.vidarr.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A workflow being handled by a processor
 *
 * @param <O> the type of an operation for this workflow
 * @param <TX> the type of a transaction for the workflow
 */
public interface ActiveWorkflow<O extends ActiveOperation<TX>, TX> {

  /** Get the caller-supplied arguments to the workflow run */
  JsonNode arguments();

  /** Get the clean-up launch information for the workflow run */
  JsonNode cleanup();

  /**
   * Set the clean-up launch information for the workflow
   *
   * @param cleanupState the clean up information required by the {@link
   *     ca.on.oicr.gsi.vidarr.WorkflowEngine}
   * @param transaction the transaction to update the information in
   */
  void cleanup(JsonNode cleanupState, TX transaction);

  /** Get the caller-supplied engine argument to the workflow run */
  ObjectNode engineArguments();

  /** Get the external IDs associated with the files in this workflow run */
  List<ExternalId> externalIds();

  /**
   * Set the external IDs associated with the files in this workflow run
   *
   * @param requiredExternalIds the IDs required by the workflow
   */
  void externalIds(List<ExternalId> requiredExternalIds);

  /**
   * Check if extra input is handled
   *
   * @return true if the workflow run as <tt>REMAINING</tt> provision out tasks
   */
  boolean extraInputIdsHandled();

  /**
   * Record if the workflow run has extra input parameters
   *
   * @param extraInputIdsHandled true if the workflow run as <tt>REMAINING</tt> provision out tasks
   * @param transaction the transaction to update the information in
   */
  void extraInputIdsHandled(boolean extraInputIdsHandled, TX transaction);

  /**
   * Get the Vidarr ID for this workflow
   *
   * <p>Plugins may use this ID for identification or debugging purposes
   */
  String id();

  /** Get the external IDs associated with this workflow run */
  Set<? extends ExternalId> inputIds();

  /**
   * Set the external IDs associated with this workflow run
   *
   * @param inputIds the input IDs
   * @param transaction the transaction to update the information in
   */
  void inputIds(Set<ExternalKey> inputIds, TX transaction);

  /**
   * Check if all completed preflight tests have passed
   *
   * <p>This should be true unless {@link #preflightFailed(Object)} has been called
   */
  boolean isPreflightOkay();

  /** Get the caller-supplied output metadata for the workflow run */
  JsonNode metadata();

  /** Get the current phase of the workflow run */
  Phase phase();

  /**
   * Update the phase of the workflow run
   *
   * @param phase the new phase
   * @param operationInitialStates the initial states for the operations that should be written to
   *     the database
   * @param transaction the transaction to update the information in
   * @return a list of operations for each initial state, respectively
   */
  List<O> phase(Phase phase, List<JsonNode> operationInitialStates, TX transaction);

  /**
   * Indicate that a preflight check has failed
   *
   * @param transaction the transaction to update the information in
   */
  void preflightFailed(TX transaction);

  /**
   * Provision out a file
   *
   * @param ids the external IDs associated with this file
   * @param storagePath the permanent storage path of the file
   * @param md5 the MD5 hash of the file's contents
   * @param metatype the MIME type of the file
   * @param labels additional data attributes assoicated with this file
   * @param transaction the transaction to update the information in
   */
  void provisionFile(
      Set<? extends ExternalId> ids,
      String storagePath,
      String md5,
      String metatype,
      Map<String, String> labels,
      TX transaction);

  /**
   * Provision out a URL
   *
   * @param ids the external IDs associated with this file
   * @param url the URL of the data recorded in an external system
   * @param labels additional data attributes assoicated with this file
   * @param transaction the transaction to update the information in
   */
  void provisionUrl(
      Set<? extends ExternalId> ids, String url, Map<String, String> labels, TX transaction);

  /** Get the parameters after provisioning in has modified them */
  ObjectNode realInput();

  /**
   * Set the parameters after provisioning in has modified them
   *
   * @param realInput the modified parameters
   * @param transaction the transaction to update the information in
   */
  void realInput(ObjectNode realInput, TX transaction);

  /**
   * Set the external URL of the workflow engine record for the workflow run
   *
   * @param workflowRunUrl the backing workflow engine's workflow identification
   * @param transaction the transaction to update the information in
   */
  void runUrl(String workflowRunUrl, TX transaction);

  /**
   * Indicate that the entire workflow run was successful and it can be exported to analysis
   * provenance
   *
   * @param transaction the transaction to update the information in
   */
  void succeeded(TX transaction);
}
