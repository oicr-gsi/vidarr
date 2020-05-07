package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.WorkMonitor;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Start a task for a workflow run once the matching operation is available
 *
 * @param <T> the return type of the operation
 */
public interface TaskStarter<T> {
  /**
   * Start the operation
   *
   * @param workflowLanguage the workflow language
   * @param workflowRunId the workflow run ID
   * @param operation the operation that the task will use to complete its result
   * @return the initial recovery state to be serialised to the database
   */
  JsonNode start(
      WorkflowLanguage workflowLanguage, String workflowRunId, WorkMonitor<T, JsonNode> operation);
}
