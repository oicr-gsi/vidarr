import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellOutputProvisioner;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine;

/** Provides an implementation of Vidarr plugins for workflow execution and provisioning out */
module ca.on.oicr.gsi.vidarr.cromwell {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires tools.jackson.core;
  requires tools.jackson.databind;
  requires com.fasterxml.jackson.annotation;
  requires java.xml;
  requires java.net.http;
  requires simpleclient;

  opens ca.on.oicr.gsi.vidarr.cromwell to
      tools.jackson.databind;

  provides WorkflowEngineProvider with
      CromwellWorkflowEngine;
  provides OutputProvisionerProvider with
      CromwellOutputProvisioner;
}
