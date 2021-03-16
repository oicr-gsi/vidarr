package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.file.Path;

public class NiassaOutputProvisioner implements OutputProvisioner {
    // look at git dir to understand structure, steal code from cromwell
    private final int[] chunks;
    static final ObjectMapper MAPPER = new ObjectMapper();
    private final SSHClient client;
    private final SFTPClient sftp;


    // all these are for the TARGET. Specifically 'chunks' is the target. SOURCE comes from workflow? I think?
    public NiassaOutputProvisioner(int[] chunks, String username, String hostname, short port) throws IOException {
        this.chunks = chunks;
        client = new SSHClient();
        client.loadKnownHosts();
        client.connect(hostname, port);
        client.authPublickey(username);
        sftp = client.newSFTPClient();
    }

    @Override
    public boolean canProvision(OutputProvisionFormat format) {
        return format == OutputProvisionFormat.FILES;
    }

    @Override
    public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
        //skip
    }

    @Override
    public JsonNode preflightCheck(JsonNode metadata, WorkMonitor<Boolean, JsonNode> monitor) {
        // copy from CromwellOutputProvisioner
        monitor.scheduleTask(() -> monitor.complete(true));
        return null;
    }


    @Override
    public void preflightRecover(JsonNode state, WorkMonitor<Boolean, JsonNode> monitor) {
        // copy from CromwellOutputProvisioner
        monitor.scheduleTask(() -> monitor.complete(true));
    }

    @Override
    public JsonNode provision(String workflowRunId,
                              String data, // this is a json object inside a string, containing labels md5 &c. this comes from workflowengine
                              JsonNode metadata, // jsonobject, contains root of TARGET. Append chunks to this. this is the 'outputDirectory' defined in typeFor and comes from shesmu
                              WorkMonitor<Result, JsonNode> monitor) {
        monitor.scheduleTask(() -> {
            Path path = Path.of(metadata.get("outputDirectory").asText());
            int startIndex = 0;
            for (final int length: chunks){
                if (length < 1) break;
                final int endIndex = Math.min(workflowRunId.length(), startIndex + length);
                if (endIndex == startIndex) break;
                path = path.resolve(workflowRunId.substring(startIndex, endIndex));
                startIndex = endIndex;
            }

            //recover: use the ssh connection to create symlink to the TARGET?

            //When done, monitor.complete with Result of type file describing symlinked file

            //and return... nothing? unclear

        });
    }

    @Override
    public void recover(JsonNode state, WorkMonitor<Result, JsonNode> monitor) {
        // Schedule a task that fails (permanentFailure)
        monitor.scheduleTask(() -> monitor.permanentFailure("Dummy action."));
    }

    @Override
    public String type() {
        return "niassa";
    }

    @Override
    public BasicType typeFor(OutputProvisionFormat format) {
        if (format == OutputProvisionFormat.FILES) {
            return BasicType.object(new Pair<>("outputDirectory", BasicType.STRING));
        } else {
            throw new IllegalArgumentException("Cannot provision non-file output");
        }
    }
}