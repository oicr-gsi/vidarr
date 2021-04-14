/** Command line interface for Vidarr */
module ca.on.oicr.gsi.vidarr.cli {
  uses ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
  uses ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
  uses ca.on.oicr.gsi.vidarr.InputProvisionerProvider;
  uses ca.on.oicr.gsi.vidarr.RuntimeProvisionerProvider;

  exports ca.on.oicr.gsi.vidarr.cli;

  requires ca.on.oicr.gsi.vidarr.core;
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.databind;
  requires info.picocli;

  opens ca.on.oicr.gsi.vidarr.cli to
      com.fasterxml.jackson.annotation,
      com.fasterxml.jackson.core,
      com.fasterxml.jackson.databind,
      info.picocli;
}
