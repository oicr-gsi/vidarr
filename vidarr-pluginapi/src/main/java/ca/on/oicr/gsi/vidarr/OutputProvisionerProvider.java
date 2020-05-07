package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Reads JSON configuration and instantiates provisioners appropriately */
public interface OutputProvisionerProvider {

  /**
   * Prepare a provisioner from a JSON configuration
   *
   * @param node the JSON data
   * @return the workflow engine
   */
  OutputProvisioner readConfiguration(ObjectNode node);

  /** Get the name for this configuration in JSON files */
  String type();
}
