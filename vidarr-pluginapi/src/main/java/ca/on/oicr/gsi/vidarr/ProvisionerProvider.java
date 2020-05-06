package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;

/** Reads JSON configuration and instantiates provisioners appropriately */
public interface ProvisionerProvider {

  /**
   * Prepare a provisioner from a JSON configuration
   *
   * @param node the JSON data
   * @return the workflow engine
   */
  Optional<Provisioner> readConfiguration(ObjectNode node);

  /** Get the name for this configuration in JSON files */
  String type();
}
