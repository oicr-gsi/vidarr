package ca.on.oicr.gsi.vidarr.server.remote;

import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.InputProvisionerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RemoteInputProvisionerProvider implements InputProvisionerProvider {
  @Override
  public InputProvisioner readConfiguration(ObjectNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String type() {
    return "remote";
  }
}
