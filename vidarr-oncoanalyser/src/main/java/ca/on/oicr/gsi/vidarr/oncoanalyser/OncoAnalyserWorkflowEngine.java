package ca.on.oicr.gsi.vidarr.oncoanalyser;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import javax.xml.stream.XMLStreamException;
import java.util.Optional;
import java.util.stream.Stream;

public class OncoAnalyserWorkflowEngine implements WorkflowEngine<StateUnstarted, CleanupState> {

    public static WorkflowEngineProvider provider(){
        return () -> Stream.of(new Pair<>("oncoanalyser", OncoAnalyserWorkflowEngine.class));
    }

    @Override
    public OperationAction<?, CleanupState, Void> cleanup() {
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
}
