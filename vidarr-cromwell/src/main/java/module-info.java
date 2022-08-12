import ca.on.oicr.gsi.vidarr.LogFileStasherProvider;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellLogFileStasher;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellOutputProvisioner;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine;

/** Provides an implementation of Vidarr plugins for workflow execution and provisioning out */
module ca.on.oicr.gsi.vidarr.cromwell {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires sshj;
  requires java.xml;
  requires java.net.http;
  requires simpleclient;

  opens ca.on.oicr.gsi.vidarr.cromwell to
      com.fasterxml.jackson.databind;

  provides WorkflowEngineProvider with
      CromwellWorkflowEngine;
  provides OutputProvisionerProvider with
      CromwellOutputProvisioner;
  provides LogFileStasherProvider with
      CromwellLogFileStasher;
}
