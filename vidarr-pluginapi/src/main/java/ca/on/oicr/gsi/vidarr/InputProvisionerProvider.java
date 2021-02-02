package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Reads JSON configuration and instantiates input provisioners appropriately */
public interface InputProvisionerProvider {

  /**
   * Prepare an input provisioner from a JSON configuration
   *
   * @param node the JSON data
   * @return the input provisioner
   */
  InputProvisioner readConfiguration(ObjectNode node);

  /** Get the name for this configuration in JSON files */
  String type();
}
