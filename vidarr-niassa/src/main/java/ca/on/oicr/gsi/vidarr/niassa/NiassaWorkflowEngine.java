package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.xml.stream.XMLStreamException;
import java.util.Optional;
import java.util.stream.Stream;

public class NiassaWorkflowEngine implements WorkflowEngine {
    @Override
    public JsonNode cleanup(JsonNode cleanupState, WorkMonitor<Void, JsonNode> monitor) {
        monitor.scheduleTask(() -> monitor.complete(null));
        return null;
    }

    @Override
    public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {

    }

    @Override
    public Optional<BasicType> engineParameters() {
        return Optional.empty();
    }

    @Override
    public void recover(JsonNode state, WorkMonitor<Result<JsonNode>, JsonNode> monitor) {
        monitor.scheduleTask(() -> monitor.permanentFailure(null));
    }

    @Override
    public void recoverCleanup(JsonNode state, WorkMonitor<Void, JsonNode> monitor) {
        // TODO ???
    }

    @Override
    public JsonNode run(WorkflowLanguage workflowLanguage,
                        String workflow,
                        Stream<Pair<String, String>> accessoryFiles,
                        String vidarrId,
                        ObjectNode workflowParameters,
                        JsonNode engineParameters,
                        WorkMonitor<Result<JsonNode>, JsonNode> monitor) {
        return null;
    }

    @Override
    public boolean supports(WorkflowLanguage language) {
        return language == WorkflowLanguage.NIASSA;
    }
}
