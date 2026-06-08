package ca.on.oicr.gsi.vidarr.sh;

import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.require;
import static ca.on.oicr.gsi.vidarr.OperationStep.require;
import static ca.on.oicr.gsi.vidarr.OperationStep.subprocess;
import static ca.on.oicr.gsi.vidarr.ProcessOutputHandler.readOutput;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.ProcessInput;
import ca.on.oicr.gsi.vidarr.ProcessOutput;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import java.util.Optional;
import java.util.stream.Stream;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** Run commands using UNIX shell locally */
public final class UnixShellWorkflowEngine implements WorkflowEngine<StateInitial, CleanupState> {
  private static final JsonMapper MAPPER =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .build();

  public static WorkflowEngineProvider provider() {
    return () -> Stream.of(new Pair<>("sh", UnixShellWorkflowEngine.class));
  }

  public UnixShellWorkflowEngine() {}

  @Override
  public OperationAction<?, CleanupState, Void> cleanup() {
    return OperationAction.value(CleanupState.class, null);
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) {
    // Do nothing.
  }

  @Override
  public Optional<BasicType> engineParameters() {
    return Optional.empty();
  }

  @Override
  public OperationAction<?, StateInitial, Result<CleanupState>> build() {
    return OperationAction.load(
            StateInitial.class,
            state ->
                new ProcessInput(
                    Optional.of(MAPPER.writeValueAsBytes(state.input())),
                    Optional.empty(),
                    "sh",
                    "-c",
                    state.workflow()))
        .then(
            require(
                (state, process) -> !state.hasAccessoryFiles(),
                "Cannot run shell with accessory files."))
        .then(subprocess(readOutput(JsonNode.class, true)))
        .then(require(ProcessOutput::success, "Process failed"))
        .map(output -> new Result<>(output.standardOutput(), "/", Optional.empty()));
  }

  @Override
  public StateInitial prepareInput(
      WorkflowLanguage workflowLanguage,
      String workflow,
      Stream<Pair<String, String>> accessoryFiles,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters) {
    return new StateInitial(workflow, workflowParameters, accessoryFiles.findAny().isPresent());
  }

  @Override
  public void startup() {
    // Always ok.
  }

  @Override
  public boolean supports(WorkflowLanguage language) {
    return language == WorkflowLanguage.UNIX_SHELL;
  }
}
