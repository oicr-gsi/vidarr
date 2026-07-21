package ca.on.oicr.gsi.vidarr.oncoanalyser;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import javax.xml.stream.XMLStreamException;
import java.util.Optional;
import java.util.stream.Stream;

public class OncoAnalyserWorkflowEngine implements WorkflowEngine<StateUnstarted, CleanupState> {

    public static WorkflowEngineProvider provider(){
        return () -> Stream.of(new Pair<>("oncoanalyser", OncoAnalyserWorkflowEngine.class));
    }

    static final JsonMapper MAPPER =
            JsonMapper.builder()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .build();

    private String statusBucket, metadataBucket, manifestBucket;

    @Override
    public OperationAction<?, CleanupState, Void> cleanup() {
        return null;
    }

    @Override
    public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
        sectionRenderer.line("Bucket containing status.json", statusBucket);
        sectionRenderer.line("Bucket containing metadata.json", metadataBucket);
        sectionRenderer.line("Bucket containing manifest.json", manifestBucket);
    }

    @Override
    public Optional<BasicType> engineParameters() {
        return Optional.empty();
    }

    @Override
    public OperationAction<?, StateUnstarted, Result<CleanupState>> build() {
        return null;
    }

    @Override
    public StateUnstarted prepareInput(WorkflowLanguage workflowLanguage, String workflow, Stream<Pair<String, String>> accessoryFiles, String vidarrId, ObjectNode workflowParameters, JsonNode engineParameters) {
        return null;
    }

    @Override
    public void startup() {

    }

    @Override
    public boolean supports(WorkflowLanguage language) {
        return false;
    }

    public void setStatusBucket(String statusBucket) {
        this.statusBucket = statusBucket;
    }

    public void setMetadataBucket(String metadataBucket) {
        this.metadataBucket = metadataBucket;
    }

    public void setManifestBucket(String manifestBucket) {
        this.manifestBucket = manifestBucket;
    }
}
