package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Reads JSON configuration and instantiates consumable resource appropriately */
public interface ConsumableResourceProvider {
  /**
   * Prepare a consumable resource from a JSON configuration
   *
   * @param name the name that should be returned by {@link ConsumableResource#name()}
   * @param node the JSON data
   * @return the consumable resource
   */
  ConsumableResource readConfiguration(String name, ObjectNode node);

  /** Get the name for this configuration in JSON files */
  String type();
}
