package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Set;

/**
 * A workflow being handled by a processor
 *
 * @param <O> the type of an operation for this workflow
 * @param <TX> the type of a transaction for the workflow
 */
public interface ActiveWorkflow<O extends ActiveOperation<TX>, TX>
    extends OutputProvisioningHandler<TX> {

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

  /**
   * Get the caller-supplied engine argument to the workflow run
   *
   * @return
   */
  JsonNode engineArguments();

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
  Set<ExternalId> inputIds();

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
  List<O> phase(Phase phase, List<Pair<String, JsonNode>> operationInitialStates, TX transaction);

  /**
   * Indicate that a preflight check has failed
   *
   * @param transaction the transaction to update the information in
   */
  void preflightFailed(TX transaction);

  /**
   * Set the parameters after provisioning in has modified them
   *
   * @param realInput the modified parameters
   * @param transaction the transaction to update the information in
   */
  void realInput(List<ObjectNode> realInput, TX transaction);

  /** Increment the current real input index and return it */
  int realInputTryNext(TX transaction);

  /** Get all variations of the parameters after provisioning in has modified them */
  List<ObjectNode> realInputs();

  /** Get the external IDs associated with the files in this workflow run */
  Set<ExternalId> requestedExternalIds();

  /**
   * Set the external IDs associated with the files in this workflow run that were mentioned
   * explicitly rather than by an <tt>ALL</tt> or <tt>REMAINING</tt>
   *
   * @param requiredExternalIds the IDs required by the workflow
   * @param transaction the transaction to update the information in
   */
  void requestedExternalIds(Set<ExternalId> requiredExternalIds, TX transaction);

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
