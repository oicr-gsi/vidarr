package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.ActiveOperation;
import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OperationControlFlow;
import ca.on.oicr.gsi.vidarr.OperationStatus;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner.ResultVisitor;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngine.Result;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class BaseProcessor<
    W extends ActiveWorkflow<PO, TX>, PO extends ActiveOperation<TX>, TX>
    implements TransactionManager<TX>, FileResolver {

  <State extends Record, Output> OperationControlFlow<State, Output> createNext(
      PO operation, TerminalHandler<Output> handler) {
    return new TerminalOperationControlFlow<>(operation, handler);
  }

  @Override
  public abstract void inTransaction(Consumer<TX> transaction);

  @Override
  public final void scheduleTask(Runnable task) {
    executor.execute(task);
  }

  @Override
  public final void scheduleTask(long delay, TimeUnit units, Runnable task) {
    executor.schedule(task, delay, units);
  }

  private interface PhaseManager<W, R, N, PO> {

    TerminalHandler<R> createTerminal(PO operation);

    WorkflowDefinition definition();

    Phase phase();

    PhaseManager<W, N, ?, PO> startNext(int size);

    W workflow();
  }

  interface TerminalHandler<Output> {

    void failed();

    JsonNode serialize(Output output);

    void succeeded(Output output);
  }

  private final class TerminalOperationControlFlow<State extends Record, Output>
      implements OperationControlFlow<State, Output> {

    private boolean finished;
    private final PO operation;
    private final TerminalHandler<Output> handler;

    TerminalOperationControlFlow(PO operation, TerminalHandler<Output> handler) {
      this.operation = operation;
      this.handler = handler;
    }

    @Override
    public void cancel() {
      // Do nothing.
    }

    @Override
    public void error(String error) {
      if (finished) {
        throw new IllegalStateException("Operation is already complete.");
      }
      finished = true;
      inTransaction(
          transaction -> {
            operation.status(OperationStatus.FAILED, transaction);
            operation.error(error, transaction);
            operation.log(Level.ERROR, error);
          });
      handler.failed();
    }

    @Override
    public void next(Output output) {
      if (finished) {
        throw new IllegalStateException("Operation is already complete.");
      }
      finished = true;
      inTransaction(
          transaction -> {
            operation.status(OperationStatus.SUCCEEDED, transaction);
            operation.recoveryState(handler.serialize(output), transaction);
          });
      handler.succeeded(output);
    }

    @Override
    public JsonNode serializeNestedState(State state) {
      return mapper().valueToTree(state);
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
    public TerminalHandler<Void> createTerminal(PO operation) {
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
    public TerminalHandler<Boolean> createTerminal(PO operation) {
      return new TerminalHandler<>() {
        @Override
        public void failed() {
          release(false);
        }

        @Override
        public JsonNode serialize(Boolean result) {
          return JsonNodeFactory.instance.booleanNode(result);
        }

        @Override
        public void succeeded(Boolean result) {
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
      inTransaction(
          transaction -> {
            if (!result) {
              ok = false;
              activeWorkflow.preflightFailed(transaction);
            }
            if (outstanding.decrementAndGet() == 0) {
              if (ok) {
                final var provisionInTasks = new ArrayList<TaskStarter<JsonMutation>>();
                final Map<Integer, List<Consumer<ObjectNode>>> retryModifications =
                    definition
                        .parameters()
                        .flatMap(
                            p ->
                                activeWorkflow.arguments().has(p.name())
                                    ? p.type()
                                    .apply(
                                        new ExtractRetryValues(
                                            mapper(), activeWorkflow.arguments().get(p.name())))
                                    : Stream.empty())
                        .distinct()
                        .collect(Collectors.toMap(Function.identity(), i -> new ArrayList<>()));
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
                                            definition.language(),
                                            BaseProcessor.this,
                                            provisionInTasks::add,
                                            retryModifications)));
                          } else {
                            throw new IllegalArgumentException(
                                String.format("Missing required parameter: %s", parameter.name()));
                          }
                        });
                final var realInputs = new ArrayList<ObjectNode>();
                while (realInputs.size() < retryModifications.size() - 1) {
                  realInputs.add(realInput.deepCopy());
                }
                realInputs.add(realInput);
                for (final var entry : retryModifications.entrySet()) {
                  final var input = realInputs.get(entry.getKey());
                  for (final var modification : entry.getValue()) {
                    modification.accept(input);
                  }
                }
                activeWorkflow.realInput(realInputs, transaction);
                final var outputRequestedExternalIds =
                    new HashSet<>(activeWorkflow.requestedExternalIds());
                // In the case of EXTERNAL ids, pass to ExtractInputExternalIds which knows how to
                // make sense of whatever non-vidarr id we pass it
                final var discoveredExternalIds =
                    definition
                        .parameters()
                        .flatMap(
                            parameter ->
                                activeWorkflow.arguments().has(parameter.name())
                                    ? parameter
                                    .type()
                                    .apply(
                                        new ExtractInputExternalIds(
                                            mapper(),
                                            activeWorkflow.arguments().get(parameter.name()),
                                            BaseProcessor.this))
                                    : Stream.empty())
                        .map(
                            ei ->
                                new ExternalId(
                                    ((ExternalId) ei).getProvider(), ((ExternalId) ei).getId()))
                        .collect(Collectors.toSet());
                if (activeWorkflow
                    .extraInputIdsHandled() // Set to true when in Remaining or All case
                    ? discoveredExternalIds.containsAll(
                    outputRequestedExternalIds) // Doesn't need to be equal in this case
                    : discoveredExternalIds.equals(outputRequestedExternalIds)) {
                  startNextPhase(this, provisionInTasks, transaction);
                } else {
                  var disjoint = discoveredExternalIds.removeAll(outputRequestedExternalIds);
                  activeWorkflow.phase(Phase.FAILED, Collections.emptyList(), transaction);
                  throw new IllegalArgumentException(
                      String.format(
                          "Workflow run %s failed because the following external keys were not handled: %s",
                          activeWorkflow.id(), disjoint));
                }
              } else {
                activeWorkflow.phase(Phase.FAILED, Collections.emptyList(), transaction);
              }
            }
          });
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
    public TerminalHandler<JsonMutation> createTerminal(PO operation) {
      return new TerminalHandler<>() {
        @Override
        public void failed() {
          if (size.decrementAndGet() == 0) {
            inTransaction(
                transaction ->
                    activeWorkflow.phase(Phase.FAILED, Collections.emptyList(), transaction));
          }
        }

        @Override
        public JsonNode serialize(JsonMutation result) {
          return mapper().valueToTree(result);
        }

        @Override
        public void succeeded(JsonMutation result) {
          semaphore.acquireUninterruptibly();
          inTransaction(
              transaction -> {
                final var inputs = activeWorkflow.realInputs();
                for (final var input : inputs) {
                  final var path = result.getPath();
                  JsonNode current = input;
                  for (var i = 0; i < path.size() - 1; i++) {
                    current = path.get(i).get(current);
                  }
                  path.get(path.size() - 1).set(current, result.getResult());
                }
                activeWorkflow.realInput(inputs, transaction);
                if (size.decrementAndGet() == 0) {
                  startNextPhase(
                      Phase2ProvisionIn.this,
                      List.of(
                          TaskStarter.launch(
                              definition, activeWorkflow, target.engine(), inputs.get(0))),
                      transaction);
                }
              });
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
      implements PhaseManager<W, WorkflowEngine.Result<JsonNode>, ProvisionData, PO> {

    private final W activeWorkflow;
    private final WorkflowDefinition definition;
    private final Target target;

    public Phase3Run(Target target, WorkflowDefinition definition, int size, W activeWorkflow) {
      this.target = target;
      this.definition = definition;
      this.activeWorkflow = activeWorkflow;
      if (size > 1) {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public TerminalHandler<Result<JsonNode>> createTerminal(PO operation) {
      return new TerminalHandler<>() {
        @Override
        public void failed() {
          final var realInputs = activeWorkflow.realInputs();
          inTransaction(
              tx -> {
                final var index = activeWorkflow.realInputTryNext(tx);
                if (index < realInputs.size()) {
                  final var relaunch =
                      TaskStarter.launch(
                          definition, activeWorkflow, target.engine(), realInputs.get(index));
                  final var nextPhaseManager = new Phase3Run(target, definition, 1, activeWorkflow);
                  final var operations =
                      workflow()
                          .phase(
                              nextPhaseManager.phase(),
                              List.of(TaskStarter.toPair(relaunch, mapper())),
                              tx);
                  if (operations.size() != 1) {
                    // The backing store has decided to abandon this workflow run.
                    return;
                  }
                  relaunch.start(
                      BaseProcessor.this,
                      operations.get(0),
                      nextPhaseManager.createTerminal(operations.get(0)));
                }
              });
        }

        @Override
        public JsonNode serialize(Result<JsonNode> result) {
          final var output = mapper().createObjectNode();
          output.set("output", result.output());
          output.set("cleanupState", result.cleanupState().orElse(NullNode.getInstance()));
          output.put("workflowRunUrl", result.workflowRunUrl());
          return output;
        }

        @Override
        public void succeeded(Result<JsonNode> result) {
          if (result.output() == null) {
            inTransaction(
                transaction -> {
                  operation.status(OperationStatus.FAILED, transaction);
                  operation.recoveryState(
                      JsonNodeFactory.instance.textNode("No output from workflow"), transaction);
                });
            return;
          }
          inTransaction(
              transaction -> {
                result.cleanupState().ifPresent(c -> workflow().cleanup(c, transaction));
                workflow().runUrl(result.workflowRunUrl(), transaction);
                final var tasks = new ArrayList<TaskStarter<ProvisionData>>();
                final var allIds = workflow().inputIds();
                final var remainingIds = new HashSet<>(allIds);
                remainingIds.removeAll(workflow().requestedExternalIds());
                target
                    .runtimeProvisioners()
                    .forEach(
                        p ->
                            tasks.add(
                                TaskStarter.launch(p, allIds, Map.of(), result.workflowRunUrl())));
                if (definition
                    .outputs()
                    .allMatch(
                        output -> {
                          final var isOk = new AtomicBoolean(true);
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
                                        remainingIds,
                                        () -> isOk.set(false),
                                        activeWorkflow.id()))
                                .forEach(tasks::add);
                            return isOk.get();
                          } else {
                            return false;
                          }
                        })) {
                  startNextPhase(Phase3Run.this, tasks, transaction);
                } else {
                  workflow().phase(Phase.FAILED, Collections.emptyList(), transaction);
                }
              });
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
    public PhaseManager<W, ProvisionData, ?, PO> startNext(int size) {
      return new Phase4ProvisionOut(target, definition, size, activeWorkflow);
    }

    @Override
    public W workflow() {
      return activeWorkflow;
    }
  }

  private class Phase4ProvisionOut implements PhaseManager<W, ProvisionData, Void, PO> {

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
    public TerminalHandler<ProvisionData> createTerminal(PO operation) {
      return new TerminalHandler<>() {
        @Override
        public void failed() {
          // Do nothing.

        }

        @Override
        public JsonNode serialize(ProvisionData result) {
          final var node = mapper().createObjectNode();
          final var info = node.putObject("info");
          info.putPOJO("ids", result.ids());
          info.putPOJO("labels", result.labels());
          result
              .result()
              .visit(
                  new ResultVisitor() {
                    @Override
                    public void file(String storagePath, String checksum, String checksumType,
                        long size, String metatype) {
                      final var file = node.putObject("result");
                      file.put("path", storagePath);
                      file.put("checksum", checksum);
                      file.put("checksumType", checksumType);
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
        public void succeeded(ProvisionData result) {
          inTransaction(
              transaction -> {
                result
                    .result()
                    .visit(
                        new ResultVisitor() {
                          @Override
                          public void file(
                              String storagePath, String checksum, String checksumType, long size,
                              String metatype) {
                            workflow()
                                .provisionFile(
                                    result.ids(),
                                    storagePath,
                                    checksum,
                                    checksumType,
                                    metatype,
                                    size,
                                    result.labels(),
                                    transaction);
                          }

                          @Override
                          public void url(String url, Map<String, String> labels) {
                            workflow()
                                .provisionUrl(result.ids(), url, result.labels(), transaction);
                          }
                        });
                if (size.decrementAndGet() == 0) {
                  final var cleanup = activeWorkflow.cleanup();
                  if (cleanup == null) {
                    workflow().succeeded(transaction);
                  } else {
                    startNextPhase(
                        Phase4ProvisionOut.this,
                        List.of(TaskStarter.launchCleanup(target.engine(), cleanup)),
                        transaction);
                  }
                }
              });
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
    public TerminalHandler<Void> createTerminal(PO operation) {
      return new TerminalHandler<>() {
        @Override
        public void failed() {
        }

        @Override
        public JsonNode serialize(Void result) {
          return JsonNodeFactory.instance.nullNode();
        }

        @Override
        public void succeeded(Void result) {
          inTransaction(activeWorkflow::succeeded);
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

  public static final Pattern ANALYSIS_RECORD_ID =
      Pattern.compile(
          "vidarr:(?<instance>[a-z][a-z0-9_-]*|_)/(?:workflow/(?<name>[a-z][a-zA-Z0-9_]*)/(?<version>[0-9]+(?:\\.[0-9]+)*(?:-[0-9]+)?)/(?<workflowhash>[0-9a-fA-F]+)/run/)?(?<type>file|url)/(?<hash>[0-9a-fA-F]+)");
  public static final Pattern WORKFLOW_RECORD_ID =
      Pattern.compile(
          "vidarr:(?<instance>[a-z][a-z0-9_-]*|_)/workflow/(?<name>[a-z][a-zA-Z0-9_]*)?/(?<version>[0-9]+(?:\\.[0-9]+)*(?:-[0-9]+)?)");
  public static final Pattern WORKFLOW_RUN_ID =
      Pattern.compile(
          "vidarr:(?<instance>[a-z][a-z0-9_-]*|_)/(?:workflow/(?<name>[a-z][a-zA-Z0-9_]*)/(?<version>[0-9]+(?:\\.[0-9]+)*(?:-[0-9]+)?)/)?run/(?<hash>[0-9a-fA-F]+)");

  public static Stream<String> extractInputVidarrIds(
      ObjectMapper mapper, WorkflowDefinition definition, JsonNode arguments) {
    return definition
        .parameters()
        .flatMap(p -> p.type().apply(new ExtractInputVidarrIds(mapper, arguments.get(p.name()))))
        .distinct();
  }

  public static String hexDigits(byte[] bytes) {
    final var buffer = new StringBuilder();
    for (final var b : bytes) {
      buffer.append(String.format("%02x", b));
    }
    return buffer.toString();
  }

  public static Stream<String> validateInput(
      ObjectMapper mapper,
      Target target,
      WorkflowDefinition workflow,
      JsonNode arguments,
      JsonNode metadata,
      JsonNode engineParameters) {
    return Stream.concat(
        target.engine().supports(workflow.language())
            ? Stream.empty()
            : Stream.of("Workflow language is not supported by this target."),
        Stream.of(
                workflow
                    .outputs()
                    .flatMap(
                        o ->
                            metadata.has(o.name())
                                ? o.type()
                                .apply(
                                    new ValidateOutputMetadata(
                                        mapper,
                                        target,
                                        "\"" + o.name() + "\"",
                                        metadata.get(o.name())))
                                : Stream.of("Missing metadata attribute " + o.name())),
                workflow
                    .parameters()
                    .flatMap(
                        p -> {
                          if (arguments.has(p.name())) {
                            return p.type()
                                .apply(
                                    new CheckInputType(
                                        mapper,
                                        target,
                                        "\"" + p.name() + "\"",
                                        arguments.get(p.name())));
                          } else {
                            return Stream.of(
                                String.format(
                                    "Argument missing: %s. Only found " + "arguments %s",
                                    p.name(), arguments));
                          }
                        }),
                target
                    .engine()
                    .engineParameters()
                    .map(
                        p ->
                            p.apply(
                                new ValidateJsonToSimpleType(
                                    "\"engine parameters\"", engineParameters)))
                    .orElseGet(
                        () ->
                            engineParameters == null
                                || engineParameters.isNull()
                                || engineParameters.isEmpty()
                                ? Stream.empty()
                                : Stream.of(
                                    "Workflow engine does not support engine parameters, but they"
                                        + " are present.")))
            .flatMap(Function.identity()));
  }

  private final ScheduledExecutorService executor;

  protected BaseProcessor(ScheduledExecutorService executor) {
    this.executor = executor;
  }

  protected final ScheduledExecutorService executor() {
    return executor;
  }

  protected abstract ObjectMapper mapper();

  protected void recover(
      Target target,
      WorkflowDefinition definition,
      W workflow,
      List<PO> activeOperations,
      RecoveryType recoverType) {
    switch (workflow.phase()) {
      case WAITING_FOR_RESOURCES:
      case INITIALIZING:
        inTransaction(transaction -> start(target, definition, workflow, transaction));
        break;
      case PREFLIGHT:
        final var p1 =
            new Phase1Preflight(
                target, activeOperations.size(), workflow, definition, workflow.isPreflightOkay());
        if (activeOperations.stream().allMatch(o -> o.status().equals(OperationStatus.SUCCEEDED))) {
          p1.release(true);
        } else {
          for (final var operation : activeOperations) {
            TaskStarter.of(
                    operation.type(),
                    target
                        .provisionerFor(WorkflowOutputDataType.valueOf(operation.type()).format())
                        .buildPreflight()
                        .recover(operation.recoveryState()))
                .start(this, operation, p1.createTerminal(operation));
          }
        }
        break;
      case PROVISION_IN:
        final var p2 = new Phase2ProvisionIn(target, activeOperations.size(), definition, workflow);
        if (activeOperations.stream().allMatch(o -> o.status().equals(OperationStatus.SUCCEEDED))) {
          inTransaction(
              transaction -> {
                startNextPhase(p2, List.of(TaskStarter.launch(p2.definition(), p2.activeWorkflow,
                    target.engine(), p2.activeWorkflow.realInputs().get(0))), transaction);
              }
          );
        } else {
          for (final var operation : activeOperations) {
            PrepareInputProvisioning.recover(
                    definition.language(),
                    operation,
                    target.provisionerFor(
                        InputProvisionFormat.valueOf(operation.type().substring(1))))
                .start(this, operation, p2.createTerminal(operation));
          }
        }
        break;
      case RUNNING:
        /*
         * The constructor of Phase3Run enforces that activeOperations.size() >1 is illegal
         */
        final var p3 = new Phase3Run(target, definition, activeOperations.size(), workflow);
        if (activeOperations.stream().allMatch(o -> o.status().equals(OperationStatus.SUCCEEDED))) {
          inTransaction(
              transaction -> {
                // if cleanup state is not null, do cleanup
                JsonNode recovery = activeOperations.get(0).recoveryState();
                JsonNode cleanup = recovery.get("cleanupState");
                if (null != cleanup && !(cleanup instanceof NullNode)) {
                  workflow.cleanup(cleanup, transaction);
                }

                // build list of runtime/output provisioning tasks
                ArrayList<TaskStarter<ProvisionData>> tasks = new ArrayList<>();
                Set<ExternalId> allIds = workflow.inputIds();
                Set<ExternalId> remainingIds = new HashSet<>(allIds);
                remainingIds.removeAll(workflow.requestedExternalIds());
                JsonNode workflowRunUrl = recovery.get("workflowRunUrl");
                if (null != workflowRunUrl && !(workflowRunUrl instanceof NullNode)) {
                  target.runtimeProvisioners().forEach(
                      p ->
                          tasks.add(
                              TaskStarter.launch(p, allIds, Map.of(), workflowRunUrl.asText())
                          )
                  );

                  if (definition.outputs().allMatch(
                      output -> {
                        final AtomicBoolean isOk = new AtomicBoolean(true);
                        JsonNode jsonOutput = recovery.get("output");
                        if (jsonOutput != null && !(jsonOutput instanceof NullNode)
                            && jsonOutput.has(output.name())) {
                          output.type().apply(
                                  new PrepareOutputProvisioning(
                                      mapper(),
                                      target,
                                      jsonOutput.get(output.name()),
                                      workflow.metadata().get(output.name()),
                                      allIds,
                                      remainingIds,
                                      () -> isOk.set(false),
                                      workflow.id()))
                              .forEach(tasks::add);
                          return isOk.get();
                        } else {
                          return false;
                        }
                      }
                  )) {
                    // launch phase 4
                    startNextPhase(p3, tasks, transaction);
                  } else {
                    workflow.phase(Phase.FAILED, Collections.emptyList(), transaction);
                  }
                }
              }
          );
        } else {
          for (final var operation : activeOperations) {
            TaskStarter.of(
                    "",
                    target
                        .engine()
                        .build()
                        .map(WorkflowEngine.Result::serialize)
                        .recover(operation.recoveryState()))
                .start(this, operation, p3.createTerminal(operation));
          }
        }
        break;
      case PROVISION_OUT:
        final var p4 =
            new Phase4ProvisionOut(target, definition, activeOperations.size(), workflow);
        if (activeOperations.stream().allMatch(o -> o.status().equals(OperationStatus.SUCCEEDED))) {
          // SUCCEEDED means we've already created the file entries in the db, so all that's left to do is clean up
          inTransaction(
              transaction -> {
                final var cleanup = workflow.cleanup();
                if (cleanup == null) {
                  workflow.succeeded(transaction);
                } else {
                  startNextPhase(
                      p4,
                      List.of(TaskStarter.launchCleanup(target.engine(), cleanup)),
                      transaction
                  );
                }
              });
        } else {
          for (final var operation : activeOperations) {
            if (operation.status().equals(OperationStatus.SUCCEEDED)) {
              // only launch the non-succeeded operations
              continue;
            }
            if (operation.type().startsWith("$")) {
              TaskStarter.of(
                      operation.type(),
                      recoverType.prepare(
                          TaskStarter.wrapRuntimeProvisioner(
                              target
                                  .runtimeProvisioners()
                                  .filter(p -> p.name().equals(operation.type().substring(1)))
                                  .findAny()
                                  .orElseThrow()),
                          operation.recoveryState()))
                  .start(this, operation, p4.createTerminal(operation));
            } else {
              TaskStarter.of(
                      operation.type(),
                      recoverType.prepare(
                          TaskStarter.wrapOutputProvisioner(
                              target.provisionerFor(OutputProvisionFormat.valueOf(operation.type()))),
                          operation.recoveryState()))
                  .start(this, operation, p4.createTerminal(operation));
            }
          }
        }
        break;
      case CLEANUP:
        final var p5 = new Phase5Cleanup(definition, workflow);
        if (activeOperations.stream().allMatch(o -> o.status().equals(OperationStatus.SUCCEEDED))) {
          inTransaction(workflow::succeeded);
        } else {
          for (final var operation : activeOperations) {
            TaskStarter.of("", target.engine().cleanup().recover(operation.recoveryState()))
                .start(this, operation, p5.createTerminal(operation));
          }
        }
        break;
      case FAILED:
        throw new UnsupportedOperationException();
    }
  }

  protected void start(Target target, WorkflowDefinition definition, W workflow, TX transaction) {
    final var preflightSteps = new ArrayList<TaskStarter<Boolean>>();
    final var requestedExternalIds = new HashSet<ExternalId>();
    final var extraInputIdsHandled = new AtomicBoolean();
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
                            requestedExternalIds::add,
                            preflightSteps::add));
              } else {
                return false;
              }
            })) {
      workflow.extraInputIdsHandled(extraInputIdsHandled.get(), transaction);
      workflow.requestedExternalIds(requestedExternalIds, transaction);
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
    final var initialStates =
        nextPhaseSteps.stream().map((starter) -> TaskStarter.toPair(starter, mapper())).toList();
    final var nextPhaseManager = currentPhase.startNext(initialStates.size());
    final var operations =
        currentPhase.workflow().phase(nextPhaseManager.phase(), initialStates, transaction);
    if (operations.size() != nextPhaseSteps.size()) {
      // The backing store has decided to abandon this workflow run.
      return;
    }
    for (var index = 0; index < nextPhaseSteps.size(); index++) {
      final var operation = operations.get(index);
      nextPhaseSteps.get(index).start(this, operation, nextPhaseManager.createTerminal(operation));
    }
  }
}
