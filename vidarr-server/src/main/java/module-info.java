import ca.on.oicr.gsi.vidarr.ProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;

module ca.on.oicr.gsi.vidarr.server {
  exports ca.on.oicr.gsi.vidarr.server;

  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;

  uses WorkflowEngineProvider;
  uses ProvisionerProvider;
}
