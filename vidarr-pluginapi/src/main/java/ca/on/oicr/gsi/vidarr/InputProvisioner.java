package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.status.SectionRenderer;
import com.fasterxml.jackson.databind.JsonNode;
import javax.xml.stream.XMLStreamException;

/** A mechanism to collect output data from a workflow and push it into an appropriate data store */
public interface InputProvisioner {

  /** Checks if the provisioner can handle this type of data */
  boolean canProvision(InputProvisionFormat format);

  /** Display configuration status */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /**
   * Get the type of information required for provisioning external files
   *
   * @param format the input format
   * @return the metadata that the client must supply to be able to provision in this data
   */
  SimpleType externalTypeFor(InputProvisionFormat format);

  /**
   * Begin provisioning out a new input that was registered in Vidarr
   *
   * @param language the workflow language the output will be consumed by
   * @param id the Vidarr ID for the file
   * @param path the output path registered in Vidarr for the file
   * @param monitor the monitor structure for writing the output of the provisioning process
   * @return the initial state of the provision out process
   */
  JsonNode provision(
      WorkflowLanguage language, String id, String path, WorkMonitor<JsonNode, JsonNode> monitor);

  /**
   * Begin provisioning out a new input that was not registered in Vidarr
   *
   * @param language the workflow language the output will be consumed by
   * @param metadata the information coming from the submitter to direct provisioning
   * @param monitor the monitor structure for writing the output of the provisioning process
   * @return the initial state of the provision out process
   */
  JsonNode provisionExternal(
      WorkflowLanguage language, JsonNode metadata, WorkMonitor<JsonNode, JsonNode> monitor);

  /**
   * Restart a provisioning process from state saved in the database
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the provisioning process
   */
  void recover(JsonNode state, WorkMonitor<JsonNode, JsonNode> monitor);
}
