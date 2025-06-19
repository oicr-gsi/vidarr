package ca.on.oicr.gsi.vidarr.core;

import static ca.on.oicr.gsi.vidarr.OperationAction.load;
import static ca.on.oicr.gsi.vidarr.OperationAction.value;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import ca.on.oicr.gsi.vidarr.core.NoOpWorkflowEngine.CleanupState;
import ca.on.oicr.gsi.vidarr.core.NoOpWorkflowEngine.NoOpState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

public class NoOpWorkflowEngine implements WorkflowEngine<NoOpState, CleanupState> {

  public record NoOpState(ObjectNode params){
    // Take the inputs and send them as the outputs
    public Result<CleanupState> pipe(){
      return new Result<>(params, null, Optional.empty());
    }
  }
  public record CleanupState(){}

  public static WorkflowEngineProvider provider() {
    return () -> Stream.of(new Pair<>("no-op", NoOpWorkflowEngine.class));
  }

  @Override
  public OperationAction<?, CleanupState, Void> cleanup() {
    return value(CleanupState.class, null);
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    sectionRenderer.line("No-Op", "does nothing");
  }

  @Override
  public Optional<BasicType> engineParameters() {
    return Optional.empty();
  }

  @Override
  public OperationAction<?, NoOpState, Result<CleanupState>> build() {
    return load(NoOpState.class, NoOpState::pipe);
  }

  @Override
  public NoOpState prepareInput(WorkflowLanguage workflowLanguage, String workflow,
      Stream<Pair<String, String>> accessoryFiles, String vidarrId, ObjectNode workflowParameters,
      JsonNode engineParameters) {
    return new NoOpState(workflowParameters);
  }

  @Override
  public void startup() {

  }

  @Override
  public boolean supports(WorkflowLanguage language) {
    // Who cares what language it is, we're not going to run it
    return true;
  }
}
