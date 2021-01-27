package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.status.SectionRenderer;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import javax.xml.stream.XMLStreamException;

/** A mechanism to collect output data from a workflow and push it into an appropriate data store */
public interface OutputProvisioner {
  /** Visit the output of the provisioning process */
  interface ResultVisitor {

    /**
     * The output is a file
     *
     * @param storagePath the permanent storage path for the file
     * @param md5 the MD5 sum of the file as a hex-encoded string
     * @param size the size of the file, in bytes
     * @param metatype the MIME type of the file
     */
    void file(String storagePath, String md5, long size, String metatype);

    /**
     * The output is a reference to another system
     *
     * @param url the URL that describes how to find the output in the remote system
     */
    void url(String url, Map<String, String> labels);
  }

  /** Output information about successful provisioning */
  abstract class Result {
    /**
     * Create metadata for a file object
     *
     * @param storagePath the permanent storage path for the file
     * @param md5 the MD5 sum of the file as a hex-encoded string
     * @param size the size of the file, in bytes
     * @param metatype the MIME type of the file
     * @return the output provisioning metadata
     */
    public static Result file(String storagePath, String md5, long size, String metatype) {
      return new Result() {

        @Override
        public void visit(ResultVisitor visitor) {
          visitor.file(storagePath, md5, size, metatype);
        }
      };
    }

    /**
     * Create metadata that refers to a remote system
     *
     * @param url the URL that describes how to find the output in the remote system
     * @return the output provisioning metadata
     */
    public static Result url(String url, Map<String, String> labels) {
      return new Result() {
        private final Map<String, String> label = new TreeMap<>(labels);

        @Override
        public void visit(ResultVisitor visitor) {
          visitor.url(url, Collections.unmodifiableMap(label));
        }
      };
    }

    private Result() {}

    /**
     * Visit the output metadata of the provisioning process
     *
     * @param visitor the visitor to view the result
     */
    public abstract void visit(ResultVisitor visitor);
  }

  /** Checks if the provisioner can handle this type of data */
  boolean canProvision(OutputProvisionFormat format);

  /** Display configuration status */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /**
   * Check that the metadata provided by the submitter is valid.
   *
   * @param metadata the metadata provided by the submitter
   * @param monitor the monitor structure for writing the output of the checking process
   */
  JsonNode preflightCheck(JsonNode metadata, WorkMonitor<Boolean, JsonNode> monitor);

  /**
   * Restart a preflight check process from state saved in the database
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the provisioning process
   */
  void preflightRecover(JsonNode state, WorkMonitor<Boolean, JsonNode> monitor);

  /**
   * Begin provisioning out a new output
   *
   * @param workflowRunId the workflow run ID assigned by Vidarr
   * @param data the output coming from the workflow
   * @param metadata the information coming from the submitter to direct provisioning
   * @param monitor the monitor structure for writing the output of the provisioning process
   * @return the initial state of the provision out process
   */
  JsonNode provision(
      String workflowRunId, String data, JsonNode metadata, WorkMonitor<Result, JsonNode> monitor);

  /**
   * Restart a provisioning process from state saved in the database
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the provisioning process
   */
  void recover(JsonNode state, WorkMonitor<Result, JsonNode> monitor);

  /** Get the name for this configuration in JSON files */
  String type();

  /**
   * Get the type of information required for provisioning out the files
   *
   * @param format the input format
   * @return the metadata that the client must supply to be able to provision in this data
   */
  BasicType typeFor(OutputProvisionFormat format);
}
