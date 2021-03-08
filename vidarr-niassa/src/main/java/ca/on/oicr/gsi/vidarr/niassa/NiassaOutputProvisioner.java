package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import com.fasterxml.jackson.databind.JsonNode;

import javax.xml.stream.XMLStreamException;
import java.nio.file.Path;
import java.util.Map;

public class NiassaOutputProvisioner implements OutputProvisioner {

    private final Map<String, String> labels;
    private final String md5;
    private final Path path;
    private final long filesize;

    public NiassaOutputProvisioner(Map<String, String> labels, String md5, Path path, long filesize) {
        this.labels = labels;
        this.md5 = md5;
        this.path = path;
        this.filesize = filesize;
    }

    @Override
    public boolean canProvision(OutputProvisionFormat format) {
        return format == OutputProvisionFormat.FILES;
    }

    @Override
    public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {

    }

    @Override
    public JsonNode preflightCheck(JsonNode metadata, WorkMonitor<Boolean, JsonNode> monitor) {
        return null;
    }

    @Override
    public void preflightRecover(JsonNode state, WorkMonitor<Boolean, JsonNode> monitor) {

    }

    @Override
    public JsonNode provision(String workflowRunId, String data, JsonNode metadata, WorkMonitor<Result, JsonNode> monitor) {
        return null;
    }

    @Override
    public void recover(JsonNode state, WorkMonitor<Result, JsonNode> monitor) {

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