package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.status.SectionRenderer;
import com.fasterxml.jackson.databind.JsonNode;
import javax.xml.stream.XMLStreamException;

/**
 * A mechanism to collect metrics and log data from a workflow and push it into an appropriate data
 * store
 *
 * <p>It does not operate on individual output from a workflow run, but simply on the workflow run
 * itself, provided by an identifier that connects it to the workflow engine that ran it.
 */
public interface RuntimeProvisioner {

  /** Display configuration status */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /** The name of this plugin */
  String name();

  /**
   * Begin provisioning out a new output
   *
   * @param workflowRunUrl the URL provided by the {@link WorkflowEngine.Result#workflowRunUrl()}
   * @param monitor the monitor structure for writing the output of the checking process
   */
  JsonNode provision(
      String workflowRunUrl, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor);

  /**
   * Restart a provisioning process from state saved in the database
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the provisioning process
   */
  void recover(JsonNode state, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor);
}
