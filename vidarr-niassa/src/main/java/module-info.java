import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.niassa.NiassaOutputProvisioner;
import ca.on.oicr.gsi.vidarr.niassa.NiassaWorkflowEngine;

module ca.on.oicr.gsi.vidarr.niassa {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires java.xml;
  requires sshj;
  requires org.jooq;
  requires java.sql;
  requires HikariCP;

  opens ca.on.oicr.gsi.vidarr.niassa to
      com.fasterxml.jackson.annotation,
      com.fasterxml.jackson.core,
      com.fasterxml.jackson.databind;

  provides WorkflowEngineProvider with
      NiassaWorkflowEngine;
  provides OutputProvisionerProvider with
      NiassaOutputProvisioner;
}
