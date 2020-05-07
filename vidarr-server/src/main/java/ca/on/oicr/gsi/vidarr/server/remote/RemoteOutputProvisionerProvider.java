package ca.on.oicr.gsi.vidarr.server.remote;

import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RemoteOutputProvisionerProvider implements OutputProvisionerProvider {

  @Override
  public OutputProvisioner readConfiguration(ObjectNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String type() {
    return "remote";
  }
}
