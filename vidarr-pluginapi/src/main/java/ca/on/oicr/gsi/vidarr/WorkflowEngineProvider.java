package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Reads JSON configuration and instantiates a workflow engine appropriately */
public interface WorkflowEngineProvider {

  /**
   * Prepare a workflow engine from a JSON configuration
   *
   * @param node the JSON data
   * @return the workflow engine
   */
  WorkflowEngine readConfiguration(ObjectNode node);

  /** Get the name for this configuration in JSON files */
  String type();
}
