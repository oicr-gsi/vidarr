import ca.on.oicr.gsi.vidarr.ProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellProvisioner;
import ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine;

module ca.on.oicr.gsi.vidarr.cromwell {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;

  provides WorkflowEngineProvider with
      CromwellWorkflowEngine;
  provides ProvisionerProvider with
      CromwellProvisioner;
}
