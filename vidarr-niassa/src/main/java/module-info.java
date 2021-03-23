import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.niassa.NiassaOutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.niassa.NiassaWorkflowEngineProvider;

module ca.on.oicr.gsi.vidarr.niassa {
    requires ca.on.oicr.gsi.vidarr.pluginapi;
    requires java.xml;
    requires sshj;
    requires org.jooq;
    requires java.sql;
    requires HikariCP;

    provides WorkflowEngineProvider with NiassaWorkflowEngineProvider;
    provides OutputProvisionerProvider with NiassaOutputProvisionerProvider;
}
