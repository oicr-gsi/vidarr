package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.*;
import ca.on.oicr.gsi.vidarr.OutputProvisioner.ResultVisitor;
import ca.on.oicr.gsi.vidarr.WorkflowEngine.Result;
import ca.on.oicr.gsi.vidarr.core.PrepareOutputProvisioning.ProvisioningOutWorkMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BaseProcessor<
        W extends ActiveWorkflow<PO, TX>,
        PO extends ActiveOperation<TX>,
        TX extends SchedulerTransaction>
    implements FileResolver {
  private interface PhaseManager<W, R, N, PO> {

    WorkMonitor<R, JsonNode> createMonitor(PO operation);

    WorkflowDefinition definition();

    Phase phase();

    PhaseManager<W, N, ?, PO> startNext(int size);

    W workflow();
  }

  private abstract class BaseOperationMonitor<R> implements WorkMonitor<R, JsonNode>, FileResolver {
    private boolean finished;
    private final PO operation;

    protected BaseOperationMonitor(PO operation) {
      this.operation = operation;
    }

    @Override
    public final void log(System.Logger.Level level, String message) {
      operation.log(level, message);
    }

    @Override
    public final synchronized void complete(R result) {
      if (finished) {
        throw new IllegalStateException("Operation is already complete.");
      }
      finished = true;
      final var transaction = startTransaction();
      operation.status(OperationStatus.SUCCEEDED, transaction);
      operation.recoveryState(serialize(result), transaction);
      transaction.commit();
      succeeded(result);
    }

    protected abstract void failed();

    @Override
    public Optional<FileMetadata> pathForId(String id) {
      return BaseProcessor.this.pathForId(id);
    }

    @Override
    public final synchronized void permanentFailure(String reason) {
      if (finished) {
        throw new IllegalStateException("Operation is already complete.");
      }
      finished = true;
      final var transaction = startTransaction();
      operation.status(OperationStatus.FAILED, transaction);
      operation.recoveryState(JsonNodeFactory.instance.textNode(reason), transaction);
      transaction.commit();
      failed();
    }

    private Runnable safeWrap(Runnable task) {
      return () -> {
        try {
          task.run();
        } catch (Throwable e) {
          System.err.println("Task thew exception. Failing workflow.");
          permanentFailure(e.toString());
        }
      };
    }

    @Override
    public final synchronized void scheduleTask(long delay, TimeUnit units, Runnable task) {
      if (finished) {
        throw new IllegalStateException("Operation is already complete. Cannot schedule task.");
      }
      executor.schedule(safeWrap(task), delay, units);
    }

    @Override
    public final synchronized void scheduleTask(Runnable task) {
      if (finished) {
        throw new IllegalStateException("Operation is already complete. Cannot schedule task.");
      }
      executor.execute(safeWrap(task));
    }

    protected abstract JsonNode serialize(R result);

    @Override
    public final synchronized void storeRecoveryInformation(JsonNode state) {
      if (finished) {
        throw new IllegalStateException(
            "Operation is already complete. Cannot store recovery information.");
      }
      final var transaction = startTransaction();
      operation.recoveryState(state, transaction);
      transaction.commit();
    }

    protected abstract void succeeded(R result);

    @Override
    public final synchronized void updateState(Status status) {
      if (finished) {
        throw new IllegalStateException(
            "Operation is already complete. Cannot store recovery information.");
      }
      final var transaction = startTransaction();
      operation.status(OperationStatus.of(status), transaction);
      transaction.commit();
    }
  }

  private final class Phase0Initial implements PhaseManager<W, Void, Boolean, PO> {

    private final WorkflowDefinition definition;
    private final Target target;
    private final W workflow;

    public Phase0Initial(Target target, W workflow, WorkflowDefinition definition) {
      this.target = target;
      this.workflow = workflow;
      this.definition = definition;
    }

    @Override
    public WorkMonitor<Void, JsonNode> createMonitor(PO operation) {
      throw new UnsupportedOperationException("Not available for the initial phase");
    }

    @Override
    public WorkflowDefinition definition() {
      return definition;
    }

    @Override
    public Phase phase() {
      return Phase.INITIALIZING;
    }

    @Override
    public PhaseManager<W, Boolean, ?, PO> startNext(int size) {
      return new Phase1Preflight(target, size, workflow, definition, true);
    }

    @Override
    public W workflow() {
      return workflow;
    }
  }

  private class Phase1Preflight implements PhaseManager<W, Boolean, JsonMutation, PO> {
    private final W activeWorkflow;
    private final WorkflowDefinition definition;
    private final List<String> discoveredInputFiles = new ArrayList<>();
    private boolean ok;
    private final AtomicInteger outstanding;
    private final Target target;

    public Phase1Preflight(
        Target target, int size, W activeWorkflow, WorkflowDefinition definition, boolean ok) {
      this.target = target;
      this.activeWorkflow = activeWorkflow;
      this.definition = definition;
      this.ok = ok;
      outstanding = new AtomicInteger(size);
    }

    @Override
    public WorkMonitor<Boolean, JsonNode> createMonitor(PO operation) {
      return new BaseOperationMonitor<Boolean>(operation) {
        @Override
        protected void failed() {
          release(false);
        }

        @Override
        protected JsonNode serialize(Boolean result) {
          return JsonNodeFactory.instance.booleanNode(result);
        }

        @Override
        protected void succeeded(Boolean result) {
          release(result);
        }
      };
    }

    @Override
    public WorkflowDefinition definition() {
      return definition;
    }

    @Override
    public Phase phase() {
      return Phase.PREFLIGHT;
    }

    public void release(Boolean result) {
      final var transaction = startTransaction();
      if (!result) {
        ok = false;
        activeWorkflow.preflightFailed(transaction);
      }
      if (outstanding.decrementAndGet() == 0) {
        if (ok) {
          final var provisionInTasks = new ArrayList<TaskStarter<JsonMutation>>();
          final var realInput = mapper().createObjectNode();
          definition
              .parameters()
              .forEach(
                  parameter -> {
                    if (activeWorkflow.arguments().has(parameter.name())) {
                      realInput.set(
                          parameter.name(),
                          parameter
                              .type()
                              .apply(
                                  new PrepareInputProvisioning(
                                      target,
                                      activeWorkflow.arguments().get(parameter.name()),
                                      Stream.of(JsonPath.object(parameter.name())),
                                      id -> {
                                        final var file = BaseProcessor.this.pathForId(id);
                                        if (file.isPresent()) {
                                          discoveredInputFiles.add(id);
                                        }
                                        return file;
                                      },
                                      provisionInTasks::add)));
                    } else if (parameter.isRequired()) {
                      throw new IllegalArgumentException(
                          String.format("Missing required parameter: %s", parameter.name()));
                    }
                  });
          activeWorkflow.realInput(realInput, transaction);
          final var outputRequestedInputIds = new HashSet<>(activeWorkflow.externalIds());
          final var discoveredInputIds =
              discoveredInputFiles.stream()
                  .flatMap(i -> BaseProcessor.this.pathForId(i).orElseThrow().externalKeys())
                  .collect(Collectors.toSet());
          if (activeWorkflow.extraInputIdsHandled()
              ? discoveredInputIds.containsAll(outputRequestedInputIds)
              : discoveredInputIds.equals(outputRequestedInputIds)) {
            startNextPhase(this, provisionInTasks, transaction);
            transaction.commit();
          } else {
            activeWorkflow.phase(Phase.FAILED, Collections.emptyList(), transaction);
          }
        } else {
          activeWorkflow.phase(Phase.FAILED, Collections.emptyList(), transaction);
        }
      }
      transaction.commit();
    }

    @Override
    public PhaseManager<W, JsonMutation, ?, PO> startNext(int size) {
      return new Phase2ProvisionIn(target, size, definition, activeWorkflow);
    }

    @Override
    public W workflow() {
      return activeWorkflow;
    }
  }

  private class Phase2ProvisionIn
      implements PhaseManager<W, JsonMutation, WorkflowEngine.Result<JsonNode>, PO> {

    private final W activeWorkflow;
    private final WorkflowDefinition definition;
    private final Semaphore semaphore = new Semaphore(1);
    private final AtomicInteger size;
    private final Target target;

    public Phase2ProvisionIn(
        Target target, int size, WorkflowDefinition definition, W activeWorkflow) {
      this.target = target;
      this.size = new AtomicInteger(size);
      this.definition = definition;
      this.activeWorkflow = activeWorkflow;
    }

    @Override
    public WorkMonitor<JsonMutation, JsonNode> createMonitor(PO operation) {
      return new BaseOperationMonitor<JsonMutation>(operation) {
        @Override
        protected void failed() {
          if (size.decrementAndGet() == 0) {
            final var transaction = startTransaction();
            activeWorkflow.phase(Phase.FAILED, Collections.emptyList(), transaction);
            transaction.commit();
          }
        }

        @Override
        protected JsonNode serialize(JsonMutation result) {
          return mapper().valueToTree(result);
        }

        @Override
        protected void succeeded(JsonMutation result) {
          semaphore.acquireUninterruptibly();
          final var transaction = startTransaction();
          final var input = activeWorkflow.realInput();
          final var path = result.getPath();
          JsonNode current = input;
          for (var i = 0; i < path.size() - 1; i++) {
            current = path.get(i).get(current);
          }
          path.get(path.size() - 1).set(current, result.getResult());
          activeWorkflow.realInput(input, transaction);
          if (size.decrementAndGet() == 0) {
            startNextPhase(
                Phase2ProvisionIn.this,
                Collections.singletonList(
                    (workflowLanguage, workflowId, operation1) ->
                        target
                            .engine()
                            .run(
                                definition.language(),
                                definition.contents(),
                                activeWorkflow.id(),
                                input,
                                activeWorkflow.engineArguments(),
                                operation1)),
                transaction);
          }
          transaction.commit();
          semaphore.release();
        }
      };
    }

    @Override
    public WorkflowDefinition definition() {
      return definition;
    }

    @Override
    public Phase phase() {
      return Phase.PROVISION_IN;
    }

    @Override
    public PhaseManager<W, WorkflowEngine.Result<JsonNode>, ?, PO> startNext(int size) {
      return new Phase3Run(target, definition, size, activeWorkflow);
    }

    @Override
    public W workflow() {
      return activeWorkflow;
    }
  }

  private class Phase3Run
      implements PhaseManager<
          W, WorkflowEngine.Result<JsonNode>, Pair<ProvisionData, OutputProvisioner.Result>, PO> {

    private final W activeWorkflow;
    private final WorkflowDefinition definition;
    private final Target target;

    public Phase3Run(Target target, WorkflowDefinition definition, int size, W activeWorkflow) {
      this.target = target;
      this.definition = definition;
      this.activeWorkflow = activeWorkflow;
      if (size != 1) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public WorkMonitor<Result<JsonNode>, JsonNode> createMonitor(PO operation) {
      return new BaseOperationMonitor<Result<JsonNode>>(operation) {
        @Override
        protected void failed() {
          // Do nothing.
        }

        @Override
        protected JsonNode serialize(Result<JsonNode> result) {
          final var output = mapper().createObjectNode();
          output.set("output", result.output());
          output.set("cleanupState", result.cleanupState().orElse(NullNode.getInstance()));
          output.put("workflowRunUrl", result.workflowRunUrl());
          return output;
        }

        @Override
        protected void succeeded(Result<JsonNode> result) {
          if (result.output() == null) {
            permanentFailure("No output from workflow");
            return;
          }
          final var transaction = startTransaction();
          result.cleanupState().ifPresent(c -> workflow().cleanup(c, transaction));
          workflow().runUrl(result.workflowRunUrl(), transaction);
          final var tasks =
              new ArrayList<TaskStarter<Pair<ProvisionData, OutputProvisioner.Result>>>();
          final var allIds = workflow().inputIds();
          final var remainingIds = new HashSet<>(allIds);
          remainingIds.removeIf(
              p ->
                  activeWorkflow.externalIds().stream()
                      .anyMatch(
                          a ->
                              a.getId().equals(p.getId())
                                  && a.getProvider().equals(p.getProvider())));
          target
              .runtimeProvisioners()
              .forEach(
                  p ->
                      tasks.add(
                          WrappedMonitor.start(
                              new ProvisionData(allIds),
                              PrepareOutputProvisioning.ProvisioningOutWorkMonitor::new,
                              (language, workflowId, monitor) ->
                                  p.provision(result.workflowRunUrl(), monitor))));
          if (definition
              .outputs()
              .allMatch(
                  output -> {
                    if (result.output().has(output.name())) {
                      output
                          .type()
                          .apply(
                              new PrepareOutputProvisioning(
                                  mapper(),
                                  target,
                                  result.output().get(output.name()),
                                  activeWorkflow.metadata().get(output.name()),
                                  allIds,
                                  remainingIds))
                          .forEach(tasks::add);
                      return true;
                    } else {
                      return false;
                    }
                  })) {
            startNextPhase(Phase3Run.this, tasks, transaction);
          } else {
            workflow().phase(Phase.FAILED, Collections.emptyList(), transaction);
          }
          transaction.commit();
        }
      };
    }

    @Override
    public WorkflowDefinition definition() {
      return definition;
    }

    @Override
    public Phase phase() {
      return Phase.RUNNING;
    }

    @Override
    public PhaseManager<W, Pair<ProvisionData, OutputProvisioner.Result>, ?, PO> startNext(
        int size) {
      return new Phase4ProvisionOut(target, definition, size, activeWorkflow);
    }

    @Override
    public W workflow() {
      return activeWorkflow;
    }
  }

  private class Phase4ProvisionOut
      implements PhaseManager<W, Pair<ProvisionData, OutputProvisioner.Result>, Void, PO> {

    private final W activeWorkflow;
    private final WorkflowDefinition definition;
    private final AtomicInteger size;
    private final Target target;

    public Phase4ProvisionOut(
        Target target, WorkflowDefinition definition, int size, W activeWorkflow) {
      this.target = target;
      this.definition = definition;
      this.size = new AtomicInteger(size);
      this.activeWorkflow = activeWorkflow;
    }

    @Override
    public WorkMonitor<Pair<ProvisionData, OutputProvisioner.Result>, JsonNode> createMonitor(
        PO operation) {
      return new BaseOperationMonitor<Pair<ProvisionData, OutputProvisioner.Result>>(operation) {
        @Override
        protected void failed() {
          // Do nothing.

        }

        @Override
        protected JsonNode serialize(Pair<ProvisionData, OutputProvisioner.Result> result) {
          final var node = mapper().createObjectNode();
          node.putPOJO("info", result.first());
          result
              .second()
              .visit(
                  new ResultVisitor() {
                    @Override
                    public void file(String storagePath, String md5, long size, String metatype) {
                      final var file = node.putObject("result");
                      file.put("path", storagePath);
                      file.put("md5", md5);
                      file.put("size", size);
                      file.put("metatype", metatype);
                    }

                    @Override
                    public void url(String url, Map<String, String> labels) {
                      final var entry = node.putObject("result");
                      entry.putPOJO("url", url);
                      entry.putPOJO("labels", labels);
                    }
                  });
          return node;
        }

        @Override
        protected void succeeded(Pair<ProvisionData, OutputProvisioner.Result> result) {
          final var transaction = startTransaction();
          result
              .second()
              .visit(
                  new ResultVisitor() {
                    @Override
                    public void file(String storagePath, String md5, long size, String metatype) {
                      workflow()
                          .provisionFile(
                              result.first().getIds(),
                              storagePath,
                              md5,
                              metatype,
                              result.first().getLabels(),
                              transaction);
                    }

                    @Override
                    public void url(String url, Map<String, String> labels) {
                      workflow()
                          .provisionUrl(
                              result.first().getIds(),
                              url,
                              result.first().getLabels(),
                              transaction);
                    }
                  });
          if (size.decrementAndGet() == 0) {
            final var cleanup = activeWorkflow.cleanup();
            if (cleanup == null) {
              workflow().succeeded(transaction);
            } else {
              startNextPhase(
                  Phase4ProvisionOut.this,
                  Collections.singletonList(
                      (lang, workflowId, monitor) -> target.engine().cleanup(cleanup, monitor)),
                  transaction);
            }
          }
          transaction.commit();
        }
      };
    }

    @Override
    public WorkflowDefinition definition() {
      return definition;
    }

    @Override
    public Phase phase() {
      return Phase.PROVISION_OUT;
    }

    @Override
    public PhaseManager<W, Void, ?, PO> startNext(int size) {
      return new Phase5Cleanup(definition, activeWorkflow);
    }

    @Override
    public W workflow() {
      return activeWorkflow;
    }
  }

  private class Phase5Cleanup implements PhaseManager<W, Void, Void, PO> {

    private final W activeWorkflow;
    private final WorkflowDefinition definition;

    private Phase5Cleanup(WorkflowDefinition definition, W activeWorkflow) {
      this.definition = definition;
      this.activeWorkflow = activeWorkflow;
    }

    @Override
    public WorkMonitor<Void, JsonNode> createMonitor(PO operation) {
      return new BaseOperationMonitor<Void>(operation) {
        @Override
        protected void failed() {}

        @Override
        protected JsonNode serialize(Void result) {
          return JsonNodeFactory.instance.nullNode();
        }

        @Override
        protected void succeeded(Void result) {
          final var transaction = startTransaction();
          activeWorkflow.succeeded(transaction);
          transaction.commit();
        }
      };
    }

    @Override
    public WorkflowDefinition definition() {
      return definition;
    }

    @Override
    public Phase phase() {
      return Phase.CLEANUP;
    }

    @Override
    public PhaseManager<W, Void, ?, PO> startNext(int size) {
      throw new IllegalStateException();
    }

    @Override
    public W workflow() {
      return activeWorkflow;
    }
  }

  private final ScheduledExecutorService executor;

  protected BaseProcessor(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  protected abstract ObjectMapper mapper();

  protected void recover(
      Target target, WorkflowDefinition definition, W workflow, List<PO> activeOperations) {
    switch (workflow.phase()) {
      case INITIALIZING:
        final var transaction = startTransaction();
        start(target, definition, workflow, transaction);
        transaction.commit();
        break;
      case PREFLIGHT:
        final var p1 =
            new Phase1Preflight(
                target, activeOperations.size(), workflow, definition, workflow.isPreflightOkay());
        for (final var operation : activeOperations) {
          target
              .provisionerFor(OutputProvisionFormat.valueOf(operation.type()))
              .preflightRecover(operation.recoveryState(), p1.createMonitor(operation));
        }
        break;
      case PROVISION_IN:
        final var p2 = new Phase2ProvisionIn(target, activeOperations.size(), definition, workflow);
        for (final var operation : activeOperations) {
          WrappedMonitor.recover(
              operation.recoveryState(),
              v -> List.of(mapper().treeToValue(v, JsonPath[].class)),
              PrepareInputProvisioning.ProvisionInMonitor::new,
              target.provisionerFor(InputProvisionFormat.valueOf(operation.type()))::recover,
              p2.createMonitor(operation));
        }
        break;
      case RUNNING:
        final var p3 = new Phase3Run(target, definition, activeOperations.size(), workflow);
        for (final var operation : activeOperations) {
          target.engine().recover(operation.recoveryState(), p3.createMonitor(operation));
        }
        break;
      case PROVISION_OUT:
        final var p4 =
            new Phase4ProvisionOut(target, definition, activeOperations.size(), workflow);
        for (final var operation : activeOperations) {
          if (operation.type().startsWith("$")) {
            WrappedMonitor.recover(
                operation.recoveryState(),
                v -> mapper().treeToValue(v, ProvisionData.class),
                ProvisioningOutWorkMonitor::new,
                target
                        .runtimeProvisioners()
                        .filter(p -> p.name().equals(operation.type().substring(1)))
                        .findAny()
                        .orElseThrow()
                    ::recover,
                p4.createMonitor(operation));
          } else {
            WrappedMonitor.recover(
                operation.recoveryState(),
                v -> mapper().treeToValue(v, ProvisionData.class),
                ProvisioningOutWorkMonitor::new,
                target.provisionerFor(OutputProvisionFormat.valueOf(operation.type()))::recover,
                p4.createMonitor(operation));
          }
        }
        break;
      case CLEANUP:
        final var p5 = new Phase5Cleanup(definition, workflow);
        for (final var operation : activeOperations) {
          target.engine().recoverCleanup(operation.recoveryState(), p5.createMonitor(operation));
        }
        break;
      case FAILED:
        throw new UnsupportedOperationException();
    }
  }

  protected void start(Target target, WorkflowDefinition definition, W workflow, TX transaction) {
    final var preflightSteps = new ArrayList<TaskStarter<Boolean>>();
    final var externalIds = new ArrayList<ExternalId>();
    final var extraInputIdsHandled = new AtomicBoolean();
    final var inputIds =
        definition
            .parameters()
            .flatMap(
                p ->
                    p.isRequired() || workflow.arguments().has(p.name())
                        ? p.type()
                            .apply(
                                new ExtractInputVidarrIds(
                                    mapper(), workflow.arguments().get(p.name())))
                        : Stream.empty())
            .distinct()
            .flatMap(
                i -> this.pathForId(i).map(FileMetadata::externalKeys).orElseGet(Stream::empty))
            .collect(Collectors.toSet());
    workflow.inputIds(inputIds, transaction);
    if (definition
        .outputs()
        .allMatch(
            output -> {
              if (workflow.metadata().has(output.name())) {
                return output
                    .type()
                    .apply(
                        new PreparePreflightChecks(
                            mapper(),
                            target,
                            workflow.metadata().get(output.name()),
                            () -> extraInputIdsHandled.set(true),
                            externalIds::add,
                            preflightSteps::add));
              } else {
                return false;
              }
            })) {
      workflow.extraInputIdsHandled(extraInputIdsHandled.get(), transaction);
      workflow.externalIds(externalIds);
      startNextPhase(new Phase0Initial(target, workflow, definition), preflightSteps, transaction);

    } else {
      workflow.phase(Phase.FAILED, Collections.emptyList(), transaction);
    }
  }

  private <P extends PhaseManager<W, ?, R, PO>, R> void startNextPhase(
      P currentPhase, List<TaskStarter<R>> nextPhaseSteps, TX transaction) {
    if (nextPhaseSteps.isEmpty()) {
      currentPhase.workflow().phase(Phase.FAILED, Collections.emptyList(), transaction);
      return;
    }
    final var monitors = new ArrayList<DelayWorkMonitor<R, JsonNode>>();
    final var initialStates =
        nextPhaseSteps.stream()
            .map(
                t -> {
                  final var monitor = new DelayWorkMonitor<R, JsonNode>();
                  monitors.add(monitor);
                  return t.start(
                      currentPhase.definition().language(), currentPhase.workflow().id(), monitor);
                })
            .collect(Collectors.toList());
    final var nextPhaseManager = currentPhase.startNext(initialStates.size());
    final var operations =
        currentPhase.workflow().phase(nextPhaseManager.phase(), initialStates, transaction);
    transaction.commit();
    for (var index = 0; index < nextPhaseSteps.size(); index++) {
      final var operation = operations.get(index);
      monitors.get(index).set(nextPhaseManager.createMonitor(operation));
    }
  }

  protected abstract TX startTransaction();
}
