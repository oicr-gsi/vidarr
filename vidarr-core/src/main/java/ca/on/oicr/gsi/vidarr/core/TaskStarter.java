package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.ActiveOperation;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OperationAction.Launcher;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep.Child;
import ca.on.oicr.gsi.vidarr.OperationStatus;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.core.BaseProcessor.TerminalHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Set;

/**
 * Start a task for a workflow run once the matching operation is available
 *
 * @param <Output> the return type of the operation
 */
interface TaskStarter<Output> {
  static <OriginalState extends Record> TaskStarter<ProvisionData> launch(
      RuntimeProvisioner<OriginalState> provisioner,
      Set<? extends ExternalId> ids,
      Map<String, String> labels,
      String workflowRunUrl) {
    return of(
        "$" + provisioner.name(),
        wrapRuntimeAction(provisioner, provisioner.run())
            .launch(new RuntimeProvisionState(ids, labels, workflowRunUrl)));
  }

  static <OriginalState extends Record> TaskStarter<ProvisionData> launch(
      OutputProvisionFormat format,
      OutputProvisioner<?, OriginalState> provisioner,
      Set<? extends ExternalId> ids,
      Map<String, String> labels,
      String workflowRunId,
      String data,
      JsonNode metadata) {

    return of(
        format.name(),
        wrapOutputProvisioner(provisioner, provisioner.build())
            .launch(new OutputProvisionState(ids, labels, workflowRunId, data, metadata)));
  }

  static <OriginalState extends Record, Cleanup extends Record>
      TaskStarter<WorkflowEngine.Result<JsonNode>> launch(
          WorkflowDefinition definition,
          ActiveWorkflow<?, ?> activeWorkflow,
          WorkflowEngine<OriginalState, Cleanup> engine,
          ObjectNode input) {
    return of(
        "",
        engine
            .build()
            .map(WorkflowEngine.Result::serialize)
            .launch(
                engine.prepareInput(
                    definition.language(),
                    definition.contents(),
                    definition.accessoryFiles(),
                    activeWorkflow.id(),
                    input,
                    activeWorkflow.engineArguments())));
  }

  static <PreflightState extends Record> TaskStarter<Boolean> launch(
      WorkflowOutputDataType format,
      OutputProvisioner<PreflightState, ?> provisioner,
      JsonNode metadata) {
    return of(
        format.name(), provisioner.buildPreflight().launch(provisioner.preflightCheck(metadata)));
  }

  static <Cleanup extends Record> TaskStarter<Void> launchCleanup(
      WorkflowEngine<?, Cleanup> engine, JsonNode savedState) {
    try {
      return of("", engine.cleanup().launch(engine.cleanup().deserializeOriginal(savedState)));
    } catch (JsonProcessingException e) {
      return new TaskStarter<>() {
        @Override
        public String name() {
          return "";
        }

        @Override
        public <TX, PO extends ActiveOperation<TX>> void start(
            BaseProcessor<?, PO, TX> processor, PO operation, TerminalHandler<Void> handler) {
          processor.inTransaction(
              tx -> {
                operation.status(OperationStatus.FAILED, tx);
                operation.recoveryState(processor.mapper().valueToTree(e.getMessage()), tx);
              });
          handler.failed();
        }

        @Override
        public JsonNode state(ObjectMapper mapper) {
          return mapper.nullNode();
        }
      };
    }
  }

  static <State extends Record, Output> TaskStarter<Output> of(
      String name, Launcher<State, Output> launcher) {
    return new TaskStarter<>() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public <TX, PO extends ActiveOperation<TX>> void start(
          BaseProcessor<?, PO, TX> processor, PO operation, TerminalHandler<Output> handler) {
        processor.scheduleTask(
            () -> launcher.launch(operation, processor, processor.createNext(operation, handler)));
      }

      @Override
      public JsonNode state(ObjectMapper mapper) {
        return launcher.state();
      }
    };
  }

  static Pair<String, JsonNode> toPair(TaskStarter<?> starter, ObjectMapper mapper) {
    return new Pair<>(starter.name(), starter.state(mapper));
  }

  static <OriginalState extends Record> OperationAction<?, ?, ProvisionData> wrapOutputProvisioner(
      OutputProvisioner<?, OriginalState> provisioner) {
    return wrapOutputProvisioner(provisioner, provisioner.build());
  }

  private static <State extends Record, OriginalState extends Record>
      OperationAction<Child<OutputProvisionState, State>, OutputProvisionState, ProvisionData>
          wrapOutputProvisioner(
              OutputProvisioner<?, OriginalState> provisioner,
              OperationAction<State, OriginalState, OutputProvisioner.Result> action) {
    return OperationAction.load(OutputProvisionState.class, OutputProvisionState::workflowRunId)
        .then(
            OperationStatefulStep.subStep(
                (state, id) -> provisioner.prepareProvisionInput(id, state.data(), state.metadata()), action))
        .map(
            (state, result) ->
                new ProvisionData(state.state().ids(), state.state().labels(), result));
  }

  private static <State extends Record, OriginalState extends Record>
      OperationAction<Child<RuntimeProvisionState, State>, RuntimeProvisionState, ProvisionData>
          wrapRuntimeAction(
              RuntimeProvisioner<OriginalState> provisioner,
              OperationAction<State, OriginalState, OutputProvisioner.Result> action) {
    return OperationAction.load(RuntimeProvisionState.class, RuntimeProvisionState::workflowRunUrl)
        .then(OperationStatefulStep.subStep((state, url) -> provisioner.provision(url), action))
        .map(
            (state, result) ->
                new ProvisionData(state.state().ids(), state.state().labels(), result));
  }

  static <OriginalState extends Record>
      OperationAction<?, RuntimeProvisionState, ProvisionData> wrapRuntimeProvisioner(
          RuntimeProvisioner<OriginalState> provisioner) {
    return wrapRuntimeAction(provisioner, provisioner.run());
  }

  String name();

  <TX, PO extends ActiveOperation<TX>> void start(
      BaseProcessor<?, PO, TX> processor, PO operation, TerminalHandler<Output> handler);

  JsonNode state(ObjectMapper mapper);
}
