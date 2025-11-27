import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellOutputProvisioner;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine;

/** Provides an implementation of Vidarr plugins for workflow execution and provisioning out */
module ca.on.oicr.gsi.vidarr.cromwell {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires java.xml;
  requires java.net.http;
  requires simpleclient;
  requires com.fasterxml.jackson.annotation;

  opens ca.on.oicr.gsi.vidarr.cromwell to
      com.fasterxml.jackson.databind;

  provides WorkflowEngineProvider with
      CromwellWorkflowEngine;
  provides OutputProvisionerProvider with
      CromwellOutputProvisioner;
}
