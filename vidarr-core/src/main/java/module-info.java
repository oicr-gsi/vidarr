import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.InputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.core.ManualOverrideConsumableResource;
import ca.on.oicr.gsi.vidarr.core.MaxInFlightConsumableResource;
import ca.on.oicr.gsi.vidarr.core.OneOfInputProvisioner;
import ca.on.oicr.gsi.vidarr.core.OneOfOutputProvisioner;
import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;

/** Main implementation of Vidarr */
module ca.on.oicr.gsi.vidarr.core {
  exports ca.on.oicr.gsi.vidarr.core;

  opens ca.on.oicr.gsi.vidarr.core to
      com.fasterxml.jackson.annotation,
      com.fasterxml.jackson.core,
      com.fasterxml.jackson.databind;

  requires ca.on.oicr.gsi.serverutils;
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires java.naming;
  requires java.sql;
  requires java.xml;
  requires simpleclient;

  provides ConsumableResourceProvider with
      ManualOverrideConsumableResource,
      MaxInFlightConsumableResource;
  provides InputProvisionerProvider with
      OneOfInputProvisioner,
      RawInputProvisioner;
  provides OutputProvisionerProvider with
      OneOfOutputProvisioner;
}
