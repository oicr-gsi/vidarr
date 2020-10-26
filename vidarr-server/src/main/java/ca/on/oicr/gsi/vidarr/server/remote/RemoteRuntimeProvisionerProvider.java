package ca.on.oicr.gsi.vidarr.server.remote;

import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.RuntimeProvisionerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RemoteRuntimeProvisionerProvider implements RuntimeProvisionerProvider {
  @Override
  public RuntimeProvisioner readConfiguration(ObjectNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String name() {
    return "remote";
  }
}
