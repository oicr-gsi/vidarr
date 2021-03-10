package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.xml.stream.XMLStreamException;

public class NiassaOutputProvisioner implements OutputProvisioner {
    private final int[] chunks;
    private final String username, password, hostname;
    static final ObjectMapper MAPPER = new ObjectMapper();

    public NiassaOutputProvisioner(int[] chunks, String username, String password, String hostname) {
        this.chunks = chunks;
        this.username = username;
        this.password = password;
        this.hostname = hostname;
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
        // copy from cronwell
        monitor.scheduleTask(() -> monitor.complete(true));
        return null;
    }


    @Override
    public void preflightRecover(JsonNode state, WorkMonitor<Boolean, JsonNode> monitor) {
        // copy from Cromwelletcetc
        monitor.scheduleTask(() -> monitor.complete(true));
    }

    @Override
    public JsonNode provision(String workflowRunId, String data, JsonNode metadata, WorkMonitor<Result, JsonNode> monitor) {
        return null;
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