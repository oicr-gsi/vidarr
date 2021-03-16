import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.niassa.NiassaOutputProvisionerProvider;

module ca.on.oicr.gsi.vidarr.niassa {
    requires ca.on.oicr.gsi.vidarr.pluginapi;
    requires java.xml;
    requires sshj;

    provides OutputProvisionerProvider with NiassaOutputProvisionerProvider;
}