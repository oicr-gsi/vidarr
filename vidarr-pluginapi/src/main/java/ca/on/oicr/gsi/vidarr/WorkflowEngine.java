package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.status.SectionRenderer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import javax.xml.stream.XMLStreamException;

/** Defines an engine that knows how to execute workflows and track the results */
public interface WorkflowEngine {

  /** The output data from a workflow */
  class Result<C> {
    private final JsonNode output;
    private final String workflowRunUrl;
    private final Optional<C> cleanupState;

    /**
     * Create new workflow output data
     *
     * @param output the JSON data emitted by the workflow
     * @param workflowRunUrl the URL of the completed workflow run for provisioning metrics and logs
     * @param cleanupState a state that can be used later to trigger cleanup of the workflow's
     *     output once provisioning out has been completed
     */
    public Result(JsonNode output, String workflowRunUrl, Optional<C> cleanupState) {
      this.output = output;
      this.workflowRunUrl = workflowRunUrl;
      this.cleanupState = cleanupState;
    }

    /** The information the plugin requires to clean up the output, if required. */
    public Optional<C> cleanupState() {
      return cleanupState;
    }

    /**
     * The URL identifying the workflow run so the metrics and logs workflows can collect
     * information about the completed job.
     */
    public String workflowRunUrl() {
      return workflowRunUrl;
    }

    /** The workflow output's output */
    public JsonNode output() {
      return output;
    }
  }
  /** Display configuration status */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /**
   * Parameters that the submitter can provide to control the behaviour of the workflow engine.
   *
   * <p>Vidarr is conceptually neutral as to what goes here. This is provided as a side channel to
   * get configuration parameters to the backend.
   */
  Optional<SimpleType> engineParameters();

  /**
   * Clean up the output of a workflow (i.e., delete its on-disk output) after provisioning has been
   * completed.
   *
   * @param cleanupState the clean up state provided with the workflow's output
   * @param monitor the monitor structure for clean up process; since no output is required, supply
   *     null as the output value
   */
  JsonNode cleanup(JsonNode cleanupState, WorkMonitor<Void, JsonNode> monitor);

  /**
   * Restart a running process from state saved in the database
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the workflow process
   */
  void recover(JsonNode state, WorkMonitor<Result<JsonNode>, JsonNode> monitor);

  /**
   * Restart a running clean upprocess from state saved in the database
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the cleanup process
   */
  void recoverCleanup(JsonNode state, WorkMonitor<Void, JsonNode> monitor);

  /**
   * Start a new workflow
   *
   * @param workflowLanguage the language the workflow was written in
   * @param workflow the URL of the workflow
   * @param vidarrId the ID of the workflow being executed
   * @param workflowParameters the input parameters to the workflow
   * @param engineParameters the input configuration parameters to the workflow engine
   * @param monitor the monitor structure for writing the output of the workflow process
   * @return the initial state of the provision out process
   */
  JsonNode run(
      WorkflowLanguage workflowLanguage,
      String workflow,
      String vidarrId,
      ObjectNode workflowParameters,
      ObjectNode engineParameters,
      WorkMonitor<Result<JsonNode>, JsonNode> monitor);
}
