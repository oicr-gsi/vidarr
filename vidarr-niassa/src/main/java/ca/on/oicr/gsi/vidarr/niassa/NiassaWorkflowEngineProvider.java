package ca.on.oicr.gsi.vidarr.niassa;

import static ca.on.oicr.gsi.vidarr.niassa.NiassaWorkflowEngine.MAPPER;

import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashSet;

/**
 * Informs Vidarr that it is capable of using plugin type 'niassa'. Reads JSON configuration and
 * creates a new NiassaWorkflowEngine with that config
 */
public class NiassaWorkflowEngineProvider implements WorkflowEngineProvider {
  /**
   *
   *
   * <pre>
   * Expects json data of form:
   * {
   *   "dbUrl": str
   *   "dbName": str
   *   "dbUser": str
   *   "dbPass": str
   *   "annotations": str[]
   * }
   * </pre>
   *
   * <p>Reads this from plugin configuration file.
   *
   * @param node the JSON data
   * @return
   */
  @Override
  public WorkflowEngine readConfiguration(ObjectNode node) {
    return new NiassaWorkflowEngine(
        node.get("dbUrl").asText(),
        node.get("dbName").asText(),
        node.get("dbUser").asText(),
        node.get("dbPass").asText(),
        node.has("annotations")
            ? MAPPER.convertValue(node.get("annotations"), new TypeReference<>() {})
            : new HashSet<>());
  }

  @Override
  public String type() {
    return "niassa";
  }
}
