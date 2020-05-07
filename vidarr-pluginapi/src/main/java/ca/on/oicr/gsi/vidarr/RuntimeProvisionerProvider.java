package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Reads JSON configuration and instantiates provisioners appropriately */
public interface RuntimeProvisionerProvider {

  /**
   * Prepare a provisioner from a JSON configuration
   *
   * @param node the JSON data
   * @return the workflow engine
   */
  RuntimeProvisioner readConfiguration(ObjectNode node);

  /** The name of this plugin */
  String name();
}
