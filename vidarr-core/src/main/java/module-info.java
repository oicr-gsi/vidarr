import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.InputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.core.MaxInFlightConsumableResource;
import ca.on.oicr.gsi.vidarr.core.OneOfInputProvisioner;
import ca.on.oicr.gsi.vidarr.core.OneOfOutputProvisioner;
import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;

/** Main implementation of Vidarr */
module ca.on.oicr.gsi.vidarr.core {
  exports ca.on.oicr.gsi.vidarr.core;

  requires ca.on.oicr.gsi.serverutils;
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires java.naming;
  requires java.sql;
  requires java.xml;
  requires simpleclient;

  uses InputProvisionerProvider;
  uses OutputProvisionerProvider;
  uses WorkflowEngineProvider;

  provides ConsumableResourceProvider with
      MaxInFlightConsumableResource;
  provides InputProvisionerProvider with
      OneOfInputProvisioner,
      RawInputProvisioner;
  provides OutputProvisionerProvider with
      OneOfOutputProvisioner;
}
