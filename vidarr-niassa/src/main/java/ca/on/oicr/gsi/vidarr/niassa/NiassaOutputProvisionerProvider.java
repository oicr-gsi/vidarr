package ca.on.oicr.gsi.vidarr.niassa;

import static ca.on.oicr.gsi.vidarr.niassa.NiassaOutputProvisioner.MAPPER;

import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

/**
 * Informs Vidarr that it is capable of using plugin type 'niassa'. Reads JSON configuration and
 * creates a new NiassaOutputProvisioner with that config
 */
public class NiassaOutputProvisionerProvider implements OutputProvisionerProvider {
  /**
   * Expects json data of form: { "chunks": "username": "hostname": "port" }
   *
   * <p>Reads this from plugin configuration file.
   *
   * @param node the JSON data
   * @return
   */
  @Override
  public OutputProvisioner readConfiguration(ObjectNode node) {
    try {
      return new NiassaOutputProvisioner(
          node.has("chunks") ? MAPPER.treeToValue(node.get("chunks"), int[].class) : new int[0],
          node.get("username").asText(),
          node.get("hostname").asText(),
          node.get("port").shortValue());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String type() {
    return "niassa";
  }
}
