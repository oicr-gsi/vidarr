package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.*;
import ca.on.oicr.gsi.vidarr.OutputProvisioner.ResultVisitor;
import ca.on.oicr.gsi.vidarr.WorkflowEngine.Result;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.core.PrepareOutputProvisioning.ProvisioningOutWorkMonitor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import java.lang.System.Logger.Level;
import java.util.*;
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
    public final synchronized void complete(R result) {
      if (finished) {
        throw new IllegalStateException("Operation is already complete.");
      }
      finished = true;
      startTransaction(
          transaction -> {
            operation.status(OperationStatus.SUCCEEDED, transaction);
            operation.recoveryState(serialize(result), transaction);
          });
      succeeded(result);
    }

    protected abstract void failed();

    @Override
    public final void log(System.Logger.Level level, String message) {
      operation.log(level, message);
    }

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
      startTransaction(
          transaction -> {
            operation.status(OperationStatus.FAILED, transaction);
            operation.recoveryState(JsonNodeFactory.instance.textNode(reason), transaction);
            operation.log(Level.ERROR, reason);
          });
      failed();
    }

    private Runnable safeWrap(Runnable task) {
      return () -> {
        if (operation.isLive()) {
          try {
            task.run();
          } catch (Throwable e) {
            log(Level.ERROR, "Task threw exception. Failing workflow: " + e.getMessage());
            e.printStackTrace();
            permanentFailure(e.toString());
          }
        } else {
          log(Level.ERROR, "Task is now dead.");
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
    public void storeDebugInfo(JsonNode information) {
      if (finished) {
        throw new IllegalStateException(
            "Operation is already complete. Cannot store debugging information.");
      }
      startTransaction(transaction -> operation.debugInfo(information, transaction));
    }

    @Override
    public final synchronized void storeRecoveryInformation(JsonNode state) {
      if (finished) {
        throw new IllegalStateException(
            "Operation is already complete. Cannot store recovery information.");
      }
      startTransaction(transaction -> operation.recoveryState(state, transaction));
    }

    protected abstract void succeeded(R result);

    @Override
    public final synchronized void updateState(Status status) {
      if (finished) {
        throw new IllegalStateException(
            "Operation is already complete. Cannot store recovery information.");
      }
      startTransaction(transaction -> operation.status(OperationStatus.of(status), transaction));
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
      startTransaction(
          transaction -> {
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
                                            BaseProcessor.this,
                                            provisionInTasks::add)));
                          } else {
                            throw new IllegalArgumentException(
                                String.format("Missing required parameter: %s", parameter.name()));
                          }
                        });
                activeWorkflow.realInput(realInput, transaction);
                final Set<ExternalId> outputRequestedExternalIds =
                    new HashSet<>(activeWorkflow.requestedExternalIds());
                // In the case of EXTERNAL ids, pass to ExtractInputExternalIds which knows how to
                // make sense of whatever non-vidarr id we pass it
                final Set<ExternalId> discoveredExternalIds =
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
    public WorkMonitor<JsonMutation, JsonNode> createMonitor(PO operation) {
      return new BaseOperationMonitor<JsonMutation>(operation) {
        @Override
        protected void failed() {
          if (size.decrementAndGet() == 0) {
            startTransaction(
                transaction ->
                    activeWorkflow.phase(Phase.FAILED, Collections.emptyList(), transaction));
          }
        }

        @Override
        protected JsonNode serialize(JsonMutation result) {
          return mapper().valueToTree(result);
        }

        @Override
        protected void succeeded(JsonMutation result) {
          semaphore.acquireUninterruptibly();
          startTransaction(
              transaction -> {
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
                              new Pair<>(
                                  "",
                                  target
                                      .engine()
                                      .run(
                                          definition.language(),
                                          definition.contents(),
                                          definition.accessoryFiles(),
                                          activeWorkflow.id(),
                                          input,
                                          activeWorkflow.engineArguments(),
                                          operation1))),
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
      implements PhaseManager<
          W, WorkflowEngine.Result<JsonNode>, Pair<ProvisionData, OutputProvisioner.Result>, PO> {

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
          startTransaction(
              transaction -> {
                result.cleanupState().ifPresent(c -> workflow().cleanup(c, transaction));
                workflow().runUrl(result.workflowRunUrl(), transaction);
                final var tasks =
                    new ArrayList<TaskStarter<Pair<ProvisionData, OutputProvisioner.Result>>>();
                final var allIds = workflow().inputIds();
                final var remainingIds = new HashSet<>(allIds);
                remainingIds.removeAll(workflow().requestedExternalIds());
                target
                    .runtimeProvisioners()
                    .forEach(
                        p ->
                            tasks.add(
                                WrappedMonitor.start(
                                    new ProvisionData(allIds),
                                    PrepareOutputProvisioning.ProvisioningOutWorkMonitor::new,
                                    (language, workflowId, monitor) ->
                                        new Pair<>(
                                            "$" + p.name(),
                                            p.provision(result.workflowRunUrl(), monitor)))));
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
                                        () -> isOk.set(false)))
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
          startTransaction(
              transaction -> {
                result
                    .second()
                    .visit(
                        new ResultVisitor() {
                          @Override
                          public void file(
                              String storagePath, String md5, long size, String metatype) {
                            workflow()
                                .provisionFile(
                                    result.first().getIds(),
                                    storagePath,
                                    md5,
                                    metatype,
                                    size,
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
                            (lang, workflowId, monitor) ->
                                new Pair<>("", target.engine().cleanup(cleanup, monitor))),
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
          startTransaction(activeWorkflow::succeeded);
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
          "vidarr:(?<instance>[a-z][a-z0-9_]*|_)/(?:workflow/(?<name>[a-z][a-zA-Z0-9_]*)/(?<version>[0-9]+(?:\\.[0-9]+)*(?:-[0-9]+)?)/(?<workflowhash>[0-9a-fA-F]+)/run/)?(?<type>file|url)/(?<hash>[0-9a-fA-F]+)");
  public static final Pattern WORKFLOW_RECORD_ID =
      Pattern.compile(
          "vidarr:(?<instance>[a-z][a-z0-9_]*|_)/workflow/(?<name>[a-z][a-zA-Z0-9_]*)?/(?<version>[0-9]+(?:\\.[0-9]+)*(?:-[0-9]+)?)");
  public static final Pattern WORKFLOW_RUN_ID =
      Pattern.compile(
          "vidarr:(?<instance>[a-z][a-z0-9_]*|_)/(?:workflow/(?<name>[a-z][a-zA-Z0-9_]*)/(?<version>[0-9]+(?:\\.[0-9]+)*(?:-[0-9]+)?)/)?run/(?<hash>[0-9a-fA-F]+)");

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
                                    p.name(), arguments.toString()));
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
      Target target, WorkflowDefinition definition, W workflow, List<PO> activeOperations) {
    switch (workflow.phase()) {
      case WAITING_FOR_RESOURCES:
      case INITIALIZING:
        startTransaction(transaction -> start(target, definition, workflow, transaction));
        break;
      case PREFLIGHT:
        final var p1 =
            new Phase1Preflight(
                target, activeOperations.size(), workflow, definition, workflow.isPreflightOkay());
        for (final var operation : activeOperations) {
          target
              .provisionerFor(WorkflowOutputDataType.valueOf(operation.type()).format())
              .preflightRecover(operation.recoveryState(), p1.createMonitor(operation));
        }
        break;
      case PROVISION_IN:
        final var p2 = new Phase2ProvisionIn(target, activeOperations.size(), definition, workflow);
        for (final var operation : activeOperations) {
          WrappedMonitor.recover(
              operation.recoveryState(),
              v -> {
                try {
                  return List.of(mapper().treeToValue(v, JsonPath[].class));
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
              },
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
                v -> {
                  try {
                    return mapper().treeToValue(v, ProvisionData.class);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                },
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
                v -> {
                  try {
                    return mapper().treeToValue(v, ProvisionData.class);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                },
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
    if (operations.size() != nextPhaseSteps.size()) {
      // The backing store has decided to abandon this workflow run.
      return;
    }
    for (var index = 0; index < nextPhaseSteps.size(); index++) {
      final var operation = operations.get(index);
      monitors.get(index).set(nextPhaseManager.createMonitor(operation));
    }
  }

  protected abstract void startTransaction(Consumer<TX> operation);
}
