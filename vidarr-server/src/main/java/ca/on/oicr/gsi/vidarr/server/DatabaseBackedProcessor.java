package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.*;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.api.BulkVersionRequest;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ExternalMultiVersionKey;
import ca.on.oicr.gsi.vidarr.core.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.IOError;
import java.lang.ref.SoftReference;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public abstract class DatabaseBackedProcessor
    extends BaseProcessor<DatabaseWorkflow, DatabaseOperation, DSLContext> {

  private static class BadRecoveryTracker {
    private static final Set<String> badRecoveryIds = new HashSet<>();
    private static final Gauge badRecoveryCount =
        Gauge.build(
                "vidarr_db_processor_recovery_current_failures",
                "The number of failures in recovering database-backed operations that have not been deleted")
            .register();

    private static final Counter badRecoveryTotal =
        Counter.build(
                "vidarr_db_processor_recovery_total_failures",
                "The total number of failures in recovering database-backed operations this boot")
            .register();

    public static void add(String id) {
      badRecoveryIds.add(id);
      badRecoveryCount.set(badRecoveryIds.size());
      badRecoveryTotal.inc();
    }

    public static void remove(String id) {
      badRecoveryIds.remove(id); // does nothing if id is not in HashSet
      badRecoveryCount.set(badRecoveryIds.size()); // no change if remove did nothing
    }
  }

  public interface DeleteResultHandler<T> {

    T deleted();

    T internalError(SQLException e);

    T noWorkflowRun();

    T stillActive();
  }

  public interface SubmissionResultHandler<T> {
    boolean allowLaunch();

    T dryRunResult();

    T externalIdMismatch();

    T internalError(Exception e);

    T invalidWorkflow(Set<String> errors);

    T launched(String vidarrId, Runnable start);

    T matchExisting(String vidarrId);

    T missingExternalIdVersion();

    T missingExternalKeyVersions(String vidarrId, List<ExternalKey> missingKeys);

    T multipleMatches(List<String> matchIds);

    T reinitialise(String vidarrId, Runnable start);

    T unknownTarget(String targetName);

    T unknownWorkflow(String name, String version);

    T unresolvedIds(TreeSet<String> inputId);
  }

  protected static final class WorkflowInformation implements Iterable<String> {

    private final WorkflowDefinition definition;
    private final int id;
    private final Map<String, BasicType> labels;

    private WorkflowInformation(
        int id, WorkflowDefinition definition, SortedMap<String, BasicType> labels) {
      this.id = id;
      this.definition = definition;
      this.labels = labels;
    }

    public WorkflowDefinition definition() {
      return definition;
    }

    public int id() {
      return id;
    }

    @Override
    public Iterator<String> iterator() {
      return labels == null ? Collections.emptyIterator() : labels.keySet().iterator();
    }

    public Stream<String> validateLabels(ObjectNode providedLabels) {
      return DatabaseBackedProcessor.validateLabels(providedLabels, labels);
    }
  }

  private static final Condition IS_DEAD =
      WORKFLOW_RUN
          .COMPLETED
          .isNull()
          .and(
              ACTIVE_WORKFLOW_RUN
                  .ENGINE_PHASE
                  .in(Phase.FAILED, Phase.WAITING_FOR_RESOURCES)
                  .or(
                      DSL.exists(
                          DSL.select()
                              .from(ACTIVE_OPERATION)
                              .where(
                                  ACTIVE_OPERATION
                                      .WORKFLOW_RUN_ID
                                      .eq(ACTIVE_WORKFLOW_RUN.ID)
                                      .and(ACTIVE_OPERATION.ATTEMPT.eq(ACTIVE_WORKFLOW_RUN.ATTEMPT))
                                      .and(ACTIVE_OPERATION.STATUS.eq(OperationStatus.FAILED))))));

  public static final TypeReference<SortedMap<String, BasicType>> LABELS_JSON_TYPE =
      new TypeReference<>() {};
  static final ObjectMapper MAPPER = new ObjectMapper();
  public static final TypeReference<Map<String, OutputType>> OUTPUT_JSON_TYPE =
      new TypeReference<>() {};
  public static final TypeReference<Map<String, InputType>> PARAMETER_JSON_TYPE =
      new TypeReference<>() {};

  private static WorkflowDefinition buildDefinitionFromRecord(
      DSLContext context, org.jooq.Record record) {
    final var accessoryFiles =
        context
            .select(WORKFLOW_VERSION_ACCESSORY.FILENAME, WORKFLOW_DEFINITION.WORKFLOW_FILE)
            .from(
                WORKFLOW_VERSION_ACCESSORY
                    .join(WORKFLOW_DEFINITION)
                    .on(WORKFLOW_VERSION_ACCESSORY.WORKFLOW_DEFINITION.eq(WORKFLOW_DEFINITION.ID)))
            .where(WORKFLOW_VERSION_ACCESSORY.WORKFLOW_VERSION.eq(record.get(WORKFLOW_VERSION.ID)))
            .stream()
            .collect(Collectors.toMap(Record2::value1, Record2::value2));
    return new WorkflowDefinition(
        record.get(WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE),
        record.get(WORKFLOW_VERSION.HASH_ID),
        record.get(WORKFLOW_DEFINITION.WORKFLOW_FILE),
        accessoryFiles,
        MAPPER
            .convertValue(record.get(WORKFLOW_VERSION.PARAMETERS), PARAMETER_JSON_TYPE)
            .entrySet()
            .stream()
            .map(e -> new WorkflowDefinition.Parameter(e.getValue(), e.getKey())),
        MAPPER
            .convertValue(record.get(WORKFLOW_VERSION.METADATA), OUTPUT_JSON_TYPE)
            .entrySet()
            .stream()
            .map(e -> new WorkflowDefinition.Output(e.getValue(), e.getKey())));
  }

  private static Stream<String> checkConsumableResource(
      Map<String, JsonNode> consumableResources, Pair<String, BasicType> resource) {
    if (consumableResources == null) {
      return Stream.of(String.format("Missing consumableResources attribute"));
    }
    return consumableResources.containsKey(resource.first())
        ? resource
            .second()
            .apply(
                new ValidateJsonToSimpleType(
                    "Consumable resource: " + resource.first(),
                    consumableResources.get(resource.first())))
        : Stream.of(String.format("Missing required consumable resource %s", resource.first()));
  }

  static String computeWorkflowRunHashId(
      String name,
      ObjectNode labelsFromClient,
      Iterable<String> labelsFromWorkflow,
      TreeSet<String> inputIds,
      Collection<? extends ExternalId> externalIds) {
    try {
      final var digest = MessageDigest.getInstance("SHA-256");
      digest.update(name.getBytes(StandardCharsets.UTF_8));
      for (final var id : inputIds) {
        final var idBytes = hashFromAnalysisId(id).getBytes(StandardCharsets.UTF_8);
        digest.update(new byte[] {0});
        digest.update(idBytes);
      }
      final var sortedExternalIds = new ArrayList<>(externalIds);
      sortedExternalIds.sort(
          Comparator.comparing(ExternalId::getProvider).thenComparing(ExternalId::getId));
      for (final var id : sortedExternalIds) {
        digest.update(new byte[] {0});
        digest.update(new byte[] {0});
        digest.update(id.getProvider().getBytes(StandardCharsets.UTF_8));
        digest.update(new byte[] {0});
        digest.update(id.getId().getBytes(StandardCharsets.UTF_8));
        digest.update(new byte[] {0});
      }

      // The client may submit any number of workflow labels, but this hashing/matching only
      // takes into account the labels that the workflow is configured with.
      for (final var label : labelsFromWorkflow) {
        digest.update(new byte[] {0});
        digest.update(label.getBytes(StandardCharsets.UTF_8));
        digest.update(new byte[] {0});
        digest.update(MAPPER.writeValueAsBytes(labelsFromClient.get(label)));
      }

      return hexDigits(digest.digest());
    } catch (NoSuchAlgorithmException | JsonProcessingException e) {
      throw new IOError(e);
    }
  }

  public static Stream<String> validateLabels(
      ObjectNode providedLabels, Map<String, BasicType> labels) {
    final var providedCount = providedLabels == null ? 0 : providedLabels.size();
    final var labelCount = labels == null ? 0 : labels.size();
    if (labelCount == 0 && providedCount == 0) {
      return Stream.empty();
    }
    if (providedCount != labelCount) {
      return Stream.of(
          String.format("%d labels are provided but %d are expected.", providedCount, labelCount));
    }
    return labels.entrySet().stream()
        .flatMap(
            entry -> {
              if (providedLabels.has(entry.getKey())) {
                return entry
                    .getValue()
                    .apply(
                        new ValidateJsonToSimpleType(
                            "Label: " + entry.getKey(), providedLabels.get(entry.getKey())))
                    .map(String.format("Label %s: ", entry.getKey())::concat);
              } else {
                return Stream.of(String.format("Label %s is not provided.", entry.getKey()));
              }
            });
  }

  private final HikariDataSource dataSource;
  private final Semaphore databaseLock = new Semaphore(1);
  private final Map<Long, SoftReference<AtomicBoolean>> liveness = new ConcurrentHashMap<>();

  protected DatabaseBackedProcessor(
      ScheduledExecutorService executor, HikariDataSource dataSource) {
    super(executor);
    this.dataSource = dataSource;
  }

  private void addNewExternalKeyVersions(
      Set<ExternalKey> externalKeys,
      DSLContext transaction,
      Long workflowRunId,
      HashMap<Pair<String, String>, List<String>> knownMatches) {
    var externalVersionInsert =
        transaction
            .insertInto(EXTERNAL_ID_VERSION)
            .columns(
                EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                EXTERNAL_ID_VERSION.KEY,
                EXTERNAL_ID_VERSION.VALUE);
    for (final var externalKey : externalKeys) {
      final var matchKeys =
          knownMatches.get(new Pair<>(externalKey.getProvider(), externalKey.getId()));
      transaction
          .update(EXTERNAL_ID_VERSION)
          .set(EXTERNAL_ID_VERSION.REQUESTED, OffsetDateTime.now())
          .where(
              EXTERNAL_ID_VERSION
                  .EXTERNAL_ID_ID
                  .eq(
                      DSL.select(EXTERNAL_ID.ID)
                          .from(EXTERNAL_ID)
                          .where(
                              EXTERNAL_ID
                                  .EXTERNAL_ID_
                                  .eq(externalKey.getId())
                                  .and(EXTERNAL_ID.PROVIDER.eq(externalKey.getProvider()))
                                  .and(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(workflowRunId))))
                  .and(
                      matchKeys.stream()
                          .map(
                              k ->
                                  EXTERNAL_ID_VERSION
                                      .KEY
                                      .eq(k)
                                      .and(
                                          EXTERNAL_ID_VERSION.VALUE.eq(
                                              externalKey.getVersions().get(k))))
                          .reduce(Condition::or)
                          .orElseThrow()))
          .execute();
      for (final var entry : externalKey.getVersions().entrySet()) {
        if (matchKeys.contains(entry.getKey())) {
          continue;
        }
        externalVersionInsert =
            externalVersionInsert.values(
                DSL.field(
                    DSL.select(EXTERNAL_ID.ID)
                        .from(EXTERNAL_ID)
                        .where(
                            EXTERNAL_ID
                                .EXTERNAL_ID_
                                .eq(externalKey.getId())
                                .and(EXTERNAL_ID.PROVIDER.eq(externalKey.getProvider()))
                                .and(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(workflowRunId)))),
                DSL.val(entry.getKey()),
                DSL.val(entry.getValue()));
      }
    }
    externalVersionInsert.execute();
  }

  protected final <T> T delete(String workflowRunId, DeleteResultHandler<T> handler) {
    try (final var connection = dataSource.getConnection()) {
      return DSL.using(connection, SQLDialect.POSTGRES)
          .transactionResult(
              context -> {
                final var transaction = DSL.using(context);
                return transaction
                    .select(ACTIVE_WORKFLOW_RUN.ID, DSL.field(IS_DEAD))
                    .from(
                        ACTIVE_WORKFLOW_RUN
                            .join(WORKFLOW_RUN)
                            .on(WORKFLOW_RUN.ID.eq(ACTIVE_WORKFLOW_RUN.ID)))
                    .where(WORKFLOW_RUN.HASH_ID.eq(workflowRunId))
                    .fetchOptional()
                    .map(
                        (id_and_dead) -> {
                          if (id_and_dead.component2()) {
                            final var oldLiveness = liveness.remove(id_and_dead.component1());
                            if (oldLiveness != null) {
                              final var oldLivenessLock = oldLiveness.get();
                              if (oldLivenessLock != null) {
                                oldLivenessLock.set(false);
                              }
                            }

                            transaction
                                .delete(ANALYSIS_EXTERNAL_ID)
                                .where(
                                    ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID.in(
                                        DSL.select(EXTERNAL_ID.ID)
                                            .from(EXTERNAL_ID)
                                            .where(
                                                EXTERNAL_ID.WORKFLOW_RUN_ID.eq(
                                                    id_and_dead.component1()))))
                                .execute();
                            transaction
                                .delete(EXTERNAL_ID_VERSION)
                                .where(
                                    EXTERNAL_ID_VERSION.EXTERNAL_ID_ID.in(
                                        DSL.select(EXTERNAL_ID.ID)
                                            .from(EXTERNAL_ID)
                                            .where(
                                                EXTERNAL_ID.WORKFLOW_RUN_ID.eq(
                                                    id_and_dead.component1()))))
                                .execute();
                            transaction
                                .delete(EXTERNAL_ID)
                                .where(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(id_and_dead.component1()))
                                .execute();
                            transaction
                                .delete(ANALYSIS)
                                .where(ANALYSIS.WORKFLOW_RUN_ID.eq(id_and_dead.component1()))
                                .execute();
                            transaction
                                .delete(ACTIVE_OPERATION)
                                .where(
                                    ACTIVE_OPERATION.WORKFLOW_RUN_ID.eq(id_and_dead.component1()))
                                .execute();
                            transaction
                                .delete(ACTIVE_WORKFLOW_RUN)
                                .where(ACTIVE_WORKFLOW_RUN.ID.eq(id_and_dead.component1()))
                                .execute();
                            transaction
                                .delete(WORKFLOW_RUN)
                                .where(WORKFLOW_RUN.ID.eq(id_and_dead.component1()))
                                .execute();
                            BadRecoveryTracker.remove(workflowRunId);
                            return handler.deleted();
                          } else {
                            return handler.stillActive();
                          }
                        })
                    .orElseGet(handler::noWorkflowRun);
              });
    } catch (SQLException e) {
      e.printStackTrace();
      return handler.internalError(e);
    }
  }

  private TreeSet<ExternalId> extractExternalIds(
      JsonNode arguments, WorkflowInformation workflow, TreeSet<String> unresolvedIds) {
    return workflow
        .definition()
        .parameters()
        .<ExternalId>flatMap(
            p ->
                arguments.has(p.name())
                    ? p.type()
                        .apply(
                            new ExtractInputExternalIds(
                                MAPPER,
                                arguments.get(p.name()),
                                id -> {
                                  final var result = pathForId(id);
                                  if (result.isEmpty()) {
                                    unresolvedIds.add(id);
                                  }
                                  return result;
                                }))
                    : Stream.empty())
        .collect(
            Collectors.toCollection(
                () ->
                    new TreeSet<>(
                        Comparator.comparing(ExternalId::getProvider)
                            .thenComparing(ExternalId::getId))));
  }

  private TreeSet<String> extractWorkflowInputIds(
      JsonNode arguments, WorkflowInformation workflow) {
    return workflow
        .definition()
        .parameters()
        .flatMap(
            p ->
                arguments.has(p.name())
                    ? p.type().apply(new ExtractInputVidarrIds(MAPPER, arguments.get(p.name())))
                    : Stream.empty())
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private List<String> findMatchingVersionKeysMatchingExternalId(
      DSLContext transaction, Long workflowRunId, ExternalKey externalKey) {
    return transaction
        .select()
        .from(EXTERNAL_ID_VERSION)
        .where(
            EXTERNAL_ID_VERSION
                .EXTERNAL_ID_ID
                .eq(
                    DSL.select(EXTERNAL_ID.ID)
                        .from(EXTERNAL_ID)
                        .where(
                            EXTERNAL_ID
                                .EXTERNAL_ID_
                                .eq(externalKey.getId())
                                .and(EXTERNAL_ID.PROVIDER.eq(externalKey.getProvider()))
                                .and(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(workflowRunId))))
                .and(
                    externalKey.getVersions().entrySet().stream()
                        .map(
                            e ->
                                EXTERNAL_ID_VERSION
                                    .KEY
                                    .eq(e.getKey())
                                    .and(EXTERNAL_ID_VERSION.VALUE.eq(e.getValue())))
                        .reduce(Condition::or)
                        .orElseThrow()))
        .fetch(EXTERNAL_ID_VERSION.KEY);
  }

  protected final Optional<WorkflowInformation> getWorkflowByName(
      String name, String version, DSLContext transaction) throws SQLException {
    return transaction
        .select()
        .from(
            WORKFLOW_VERSION
                .join(WORKFLOW_DEFINITION)
                .on(WORKFLOW_VERSION.WORKFLOW_DEFINITION.eq(WORKFLOW_DEFINITION.ID))
                .join(WORKFLOW)
                .on(WORKFLOW.NAME.eq(WORKFLOW_VERSION.NAME)))
        .where(WORKFLOW_VERSION.NAME.eq(name).and(WORKFLOW_VERSION.VERSION.eq(version)))
        .fetchOptional()
        .map(
            record -> {
              try {
                return new WorkflowInformation(
                    record.get(WORKFLOW_VERSION.ID),
                    buildDefinitionFromRecord(transaction, record),
                    (record.get(WORKFLOW.LABELS) == null
                        ? new TreeMap<>()
                        : MAPPER.readValue(record.get(WORKFLOW.LABELS).data(), LABELS_JSON_TYPE)));
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private <T> T launchNewWorkflowRun(
      String targetName,
      String name,
      String version,
      ObjectNode labels,
      JsonNode arguments,
      JsonNode engineParameters,
      JsonNode metadata,
      Set<ExternalKey> externalKeys,
      Map<String, JsonNode> consumableResources,
      SubmissionResultHandler<T> handler,
      Target target,
      DSLContext transaction,
      WorkflowInformation workflow,
      TreeSet<String> inputIds,
      TreeSet<ExternalId> externalIds,
      String candidateId)
      throws SQLException {
    final var dbWorkflow =
        DatabaseWorkflow.create(
            targetName,
            target,
            workflow.id(),
            name,
            version,
            candidateId,
            labels,
            arguments,
            engineParameters,
            metadata,
            inputIds,
            externalIds,
            consumableResources,
            this::liveness,
            transaction);
    var externalVersionInsert =
        transaction
            .insertInto(EXTERNAL_ID_VERSION)
            .columns(
                EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                EXTERNAL_ID_VERSION.KEY,
                EXTERNAL_ID_VERSION.VALUE);
    for (final var externalKey : externalKeys) {
      for (final var entry : externalKey.getVersions().entrySet()) {
        externalVersionInsert =
            externalVersionInsert.values(
                DSL.field(
                    DSL.select(EXTERNAL_ID.ID)
                        .from(EXTERNAL_ID)
                        .where(
                            EXTERNAL_ID
                                .EXTERNAL_ID_
                                .eq(externalKey.getId())
                                .and(EXTERNAL_ID.PROVIDER.eq(externalKey.getProvider()))
                                .and(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(dbWorkflow.dbId())))),
                DSL.val(entry.getKey()),
                DSL.val(entry.getValue()));
      }
    }
    externalVersionInsert.execute();
    return handler.launched(
        candidateId,
        new ConsumableResourceChecker(
            target,
            dataSource,
            executor(),
            dbWorkflow.dbId(),
            liveness(dbWorkflow.dbId()),
            name,
            version,
            candidateId,
            consumableResources,
            new Runnable() {
              private boolean launched;

              @Override
              public void run() {
                if (launched) {
                  throw new IllegalStateException("Workflow has already been" + " launched");
                }
                launched = true;
                startTransaction(
                    runTransaction ->
                        DatabaseBackedProcessor.this.start( // runs when new workflow run submitted
                            target, workflow.definition(), dbWorkflow, runTransaction));
              }
            }));
  }

  private AtomicBoolean liveness(long workflowRunId) {
    return liveness
        .computeIfAbsent(workflowRunId, k -> new SoftReference<>(new AtomicBoolean(true)))
        .get();
  }

  @Override
  protected final ObjectMapper mapper() {
    return MAPPER;
  }

  public final void recover(Consumer<Runnable> startRaw) throws SQLException {
    try (final var connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              context -> {
                var dsl = DSL.using(context);
                var operations =
                    dsl
                        .select(ACTIVE_OPERATION.asterisk())
                        .from(ACTIVE_OPERATION)
                        .where(
                            ACTIVE_OPERATION
                                .STATUS
                                .in(OperationStatus.FAILED, OperationStatus.SUCCEEDED)
                                .not()
                                .and(
                                    ACTIVE_OPERATION.WORKFLOW_RUN_ID.in(
                                        dsl.select(ACTIVE_WORKFLOW_RUN.ID)
                                            .from(ACTIVE_WORKFLOW_RUN)
                                            .where(
                                                ACTIVE_WORKFLOW_RUN.ENGINE_PHASE.ne(Phase.FAILED))))
                                .and(
                                    ACTIVE_OPERATION.ATTEMPT.eq(
                                        DSL.select(ACTIVE_WORKFLOW_RUN.ATTEMPT)
                                            .from(ACTIVE_WORKFLOW_RUN)
                                            .where(
                                                ACTIVE_WORKFLOW_RUN.ID.eq(
                                                    ACTIVE_OPERATION.WORKFLOW_RUN_ID)))))
                        .stream()
                        .collect(
                            Collectors.groupingBy(
                                r -> r.get(ACTIVE_OPERATION.WORKFLOW_RUN_ID),
                                Collectors.mapping(
                                    r ->
                                        DatabaseOperation.recover(
                                            r, liveness(r.get(ACTIVE_OPERATION.WORKFLOW_RUN_ID))),
                                    Collectors.toList())));
                dsl.select()
                    .from(
                        ACTIVE_WORKFLOW_RUN
                            .join(WORKFLOW_RUN)
                            .on(ACTIVE_WORKFLOW_RUN.ID.eq(WORKFLOW_RUN.ID))
                            .join(WORKFLOW_VERSION)
                            .on(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID))
                            .join(WORKFLOW_DEFINITION)
                            .on(WORKFLOW_VERSION.WORKFLOW_DEFINITION.eq(WORKFLOW_DEFINITION.ID)))
                    .where(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE.ne(Phase.FAILED))
                    .forEach(
                        record ->
                            targetByName(record.get(ACTIVE_WORKFLOW_RUN.TARGET))
                                .ifPresent(
                                    target -> {
                                      try {
                                        if (record.get(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE)
                                            == Phase.WAITING_FOR_RESOURCES) {
                                          final var definition =
                                              buildDefinitionFromRecord(context.dsl(), record);
                                          final var workflow =
                                              DatabaseWorkflow.recover(
                                                  target,
                                                  record,
                                                  liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                  dsl);
                                          final var activeOperations =
                                              operations.getOrDefault(
                                                  record.get(ACTIVE_WORKFLOW_RUN.ID), List.of());
                                          for (final var operation : activeOperations) {
                                            operation.linkTo(workflow);
                                          }
                                          final var consumableResources =
                                              MAPPER.convertValue(
                                                  record.get(
                                                      ACTIVE_WORKFLOW_RUN.CONSUMABLE_RESOURCES),
                                                  new TypeReference<Map<String, JsonNode>>() {});
                                          startRaw.accept(
                                              new ConsumableResourceChecker(
                                                  target,
                                                  dataSource,
                                                  executor(),
                                                  record.get(ACTIVE_WORKFLOW_RUN.ID),
                                                  liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                  record.get(WORKFLOW.NAME),
                                                  record.get(WORKFLOW_VERSION.VERSION),
                                                  record.get(WORKFLOW_RUN.HASH_ID),
                                                  consumableResources,
                                                  () ->
                                                      recover(
                                                          target,
                                                          definition,
                                                          workflow,
                                                          activeOperations)));
                                        } else {
                                          System.err.printf(
                                              "Recovering workflow %s...\n",
                                              record.get(WORKFLOW_RUN.HASH_ID));
                                          target
                                              .consumableResources()
                                              .forEach(
                                                  cr ->
                                                      cr.second()
                                                          .recover(
                                                              record.get(WORKFLOW_VERSION.NAME),
                                                              record.get(WORKFLOW_VERSION.VERSION),
                                                              record.get(WORKFLOW_RUN.HASH_ID),
                                                              Optional.ofNullable(record.get(ACTIVE_WORKFLOW_RUN.CONSUMABLE_RESOURCES))));
                                          final var workflow =
                                              DatabaseWorkflow.recover(
                                                  target,
                                                  record,
                                                  liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                  dsl);
                                          final var activeOperations =
                                              operations.getOrDefault(
                                                  record.get(ACTIVE_WORKFLOW_RUN.ID), List.of());
                                          for (final var operation : activeOperations) {
                                            operation.linkTo(workflow);
                                          }
                                          recover(
                                              target,
                                              buildDefinitionFromRecord(context.dsl(), record),
                                              workflow,
                                              activeOperations);
                                        }
                                      } catch (Exception e) {
                                        String erroneousHash = record.get(WORKFLOW_RUN.HASH_ID);
                                        System.err.printf(
                                            "Error recovering workflow run %s: \n", erroneousHash);
                                        e.printStackTrace();
                                        BadRecoveryTracker.add(erroneousHash);
                                        System.err.println(
                                            "Continuing recovery on next record in database if one exists.");
                                      }
                                    }));
              });
      connection.commit();
    }
  }

  protected final Set<String> recoveryFailures() {
    return BadRecoveryTracker.badRecoveryIds;
  }

  protected final Optional<FileMetadata> resolveInDatabase(String inputId) {
    try {
      try (final var connection = dataSource.getConnection()) {
        return DSL
            .using(connection, SQLDialect.POSTGRES)
            .select()
            .from(
                ANALYSIS
                    .join(ANALYSIS_EXTERNAL_ID)
                    .on(ANALYSIS.ID.eq(ANALYSIS_EXTERNAL_ID.ANALYSIS_ID))
                    .join(EXTERNAL_ID)
                    .on(EXTERNAL_ID.ID.eq(ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID))
                    .join(EXTERNAL_ID_VERSION)
                    .on(EXTERNAL_ID.ID.eq(EXTERNAL_ID_VERSION.EXTERNAL_ID_ID)))
            .where(ANALYSIS.ANALYSIS_TYPE.eq("file").and(ANALYSIS.HASH_ID.like(inputId)))
            .stream()
            .collect(
                Collectors.groupingBy(
                    r -> r.get(ANALYSIS.FILE_PATH),
                    Collectors.groupingBy(
                        r ->
                            new Pair<>(
                                r.get(EXTERNAL_ID.PROVIDER), r.get(EXTERNAL_ID.EXTERNAL_ID_)),
                        Collectors.toMap(
                            r -> r.get(EXTERNAL_ID_VERSION.KEY),
                            r ->
                                Stream.of(r.get(EXTERNAL_ID_VERSION.VALUE))
                                    .collect(Collectors.toSet()),
                            (a, b) -> {
                              a.addAll(b);
                              return a;
                            }))))
            .entrySet()
            .stream()
            .<FileMetadata>map(
                e ->
                    new FileMetadata() {
                      private final List<ExternalMultiVersionKey> keys =
                          e.getValue().entrySet().stream()
                              .map(
                                  ee ->
                                      new ExternalMultiVersionKey(
                                          ee.getKey().first(), ee.getKey().second(), ee.getValue()))
                              .collect(Collectors.toList());
                      private final String path = e.getKey();

                      @Override
                      public Stream<ExternalMultiVersionKey> externalKeys() {
                        return keys.stream();
                      }

                      @Override
                      public String path() {
                        return path;
                      }
                    })
            .findAny();
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  @Override
  protected final void startTransaction(Consumer<DSLContext> operation) {
    databaseLock.acquireUninterruptibly();
    try {
      try (final var connection = dataSource.getConnection()) {
        DSL.using(connection, SQLDialect.POSTGRES)
            .transaction(context -> operation.accept(DSL.using(context)));
        connection.commit();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      databaseLock.release();
    }
  }

  protected final <T> T submit(
      String targetName,
      String name,
      String version,
      ObjectNode labels,
      JsonNode arguments,
      JsonNode engineParameters,
      JsonNode metadata,
      Set<ExternalKey> externalKeys,
      Map<String, JsonNode> consumableResources,
      int attempt,
      SubmissionResultHandler<T> handler) {
    return targetByName(targetName)
        .map(
            target -> {
              try {
                try (final var connection = dataSource.getConnection()) {
                  final var submitResult =
                      DSL.using(connection, SQLDialect.POSTGRES)
                          .transactionResult(
                              context -> {
                                final var transaction = DSL.using(context);
                                try {
                                  return getWorkflowByName(name, version, transaction)
                                      .map(
                                          workflow -> {
                                            final var errors =
                                                validateWorkflowInputs(
                                                    labels,
                                                    arguments,
                                                    engineParameters,
                                                    metadata,
                                                    consumableResources,
                                                    target,
                                                    workflow);
                                            if (!errors.isEmpty()) {
                                              return handler.invalidWorkflow(errors);
                                            }
                                            final var retryError =
                                                validateWorkflowRetry(arguments, workflow);
                                            if (retryError.isPresent()) {
                                              return handler.invalidWorkflow(
                                                  Set.of(retryError.get()));
                                            }

                                            if (workflow
                                                .definition()
                                                .outputs()
                                                .map(
                                                    output ->
                                                        output
                                                            .type()
                                                            .apply(
                                                                new CheckOutputCompatibility(
                                                                    MAPPER,
                                                                    metadata.get(output.name()))))
                                                .reduce(OutputCompatibility::worst)
                                                .map(OutputCompatibility.BROKEN::equals)
                                                .orElse(false)) {
                                              return handler.invalidWorkflow(
                                                  Set.of(
                                                      "The metadata for the workflow has external"
                                                          + " keys that are manually assigned to"
                                                          + " optional output and there is"
                                                          + " mandatory output using remaining"
                                                          + " (non-manually assigned) external"
                                                          + " keys. This is forbidden as it could"
                                                          + " potentially lose keys."));
                                            }

                                            final var inputIds =
                                                extractWorkflowInputIds(arguments, workflow);

                                            final var unresolvedIds = new TreeSet<String>();

                                            final var externalIds =
                                                extractExternalIds(
                                                    arguments, workflow, unresolvedIds);
                                            if (!unresolvedIds.isEmpty()) {
                                              return handler.unresolvedIds(unresolvedIds);
                                            }
                                            if (externalKeys.stream()
                                                .anyMatch(k -> k.getVersions().isEmpty())) {
                                              return handler.missingExternalIdVersion();
                                            }

                                            final var externalKeyIds =
                                                externalKeys.stream()
                                                    .map(
                                                        k -> new Pair<>(k.getProvider(), k.getId()))
                                                    .collect(Collectors.toSet());

                                            final var requiredOutputKeys =
                                                workflow
                                                    .definition()
                                                    .outputs()
                                                    .flatMap(
                                                        output ->
                                                            output
                                                                .type()
                                                                .apply(
                                                                    new ExtractOutputKeys(
                                                                        MAPPER,
                                                                        externalKeyIds,
                                                                        false,
                                                                        metadata.get(
                                                                            output.name()))))
                                                    .collect(Collectors.toSet());
                                            final var optionalOutputKeys =
                                                workflow
                                                    .definition()
                                                    .outputs()
                                                    .flatMap(
                                                        output ->
                                                            output
                                                                .type()
                                                                .apply(
                                                                    new ExtractOutputKeys(
                                                                        MAPPER,
                                                                        externalKeyIds,
                                                                        true,
                                                                        metadata.get(
                                                                            output.name()))))
                                                    .collect(Collectors.toSet());

                                            if (externalIds.size() != externalKeys.size()
                                                || requiredOutputKeys.size() != externalKeys.size()
                                                || !requiredOutputKeys.equals(externalKeyIds)
                                                || !externalKeyIds.containsAll(optionalOutputKeys)
                                                || !externalKeyIds.equals(
                                                    externalIds.stream()
                                                        .map(
                                                            k ->
                                                                new Pair<>(
                                                                    k.getProvider(), k.getId()))
                                                        .collect(Collectors.toSet()))) {
                                              return handler.externalIdMismatch();
                                            }
                                            try {
                                              final var candidateId =
                                                  computeWorkflowRunHashId(
                                                      name,
                                                      labels,
                                                      workflow,
                                                      inputIds,
                                                      externalIds);
                                              final var candidates =
                                                  transaction
                                                      .select(WORKFLOW_RUN.ID, WORKFLOW_RUN.HASH_ID)
                                                      .from(WORKFLOW_RUN)
                                                      .where(WORKFLOW_RUN.HASH_ID.eq(candidateId))
                                                      .stream()
                                                      .map(
                                                          r ->
                                                              new Pair<>(
                                                                  r.get(WORKFLOW_RUN.ID),
                                                                  r.get(WORKFLOW_RUN.HASH_ID)))
                                                      .collect(Collectors.toList());
                                              if (candidates.isEmpty()) {
                                                if (handler.allowLaunch()) {
                                                  return launchNewWorkflowRun(
                                                      targetName,
                                                      name,
                                                      version,
                                                      labels,
                                                      arguments,
                                                      engineParameters,
                                                      metadata,
                                                      externalKeys,
                                                      consumableResources,
                                                      handler,
                                                      target,
                                                      transaction,
                                                      workflow,
                                                      inputIds,
                                                      externalIds,
                                                      candidateId);
                                                } else {
                                                  return handler.dryRunResult();
                                                }
                                              } else if (candidates.size() == 1) {
                                                final var workflowRunId = candidates.get(0).first();
                                                final var knownMatches =
                                                    new HashMap<
                                                        Pair<String, String>, List<String>>();
                                                final var missingKeys =
                                                    new ArrayList<ExternalKey>();
                                                for (final var externalKey : externalKeys) {
                                                  final var matchKeys =
                                                      findMatchingVersionKeysMatchingExternalId(
                                                          transaction, workflowRunId, externalKey);

                                                  if (matchKeys.isEmpty()) {
                                                    missingKeys.add(externalKey);
                                                  } else {
                                                    knownMatches.put(
                                                        new Pair<>(
                                                            externalKey.getProvider(),
                                                            externalKey.getId()),
                                                        matchKeys);
                                                  }
                                                }
                                                if (!missingKeys.isEmpty()) {
                                                  return handler.missingExternalKeyVersions(
                                                      candidates.get(0).second(), missingKeys);
                                                }

                                                // Exit early if no launching is to occur (e.g. dry
                                                // run or validate mode).
                                                if (!handler.allowLaunch()) {
                                                  return handler.matchExisting(
                                                      candidates.get(0).second());
                                                }

                                                addNewExternalKeyVersions(
                                                    externalKeys,
                                                    transaction,
                                                    workflowRunId,
                                                    knownMatches);
                                                // If this workflow is active, but failed, and the
                                                // attempt number is higher or this is a different
                                                // workflow version, we should restart it.
                                                if (context
                                                        .dsl()
                                                        .selectCount()
                                                        .from(
                                                            ACTIVE_WORKFLOW_RUN
                                                                .join(WORKFLOW_RUN)
                                                                .on(
                                                                    WORKFLOW_RUN.ID.eq(
                                                                        ACTIVE_WORKFLOW_RUN.ID)))
                                                        .where(
                                                            WORKFLOW_RUN
                                                                .ID
                                                                .eq(workflowRunId)
                                                                .and(IS_DEAD)
                                                                .and(
                                                                    ACTIVE_WORKFLOW_RUN
                                                                        .ATTEMPT
                                                                        .eq(attempt - 1)
                                                                        .or(
                                                                            WORKFLOW_RUN
                                                                                .WORKFLOW_VERSION_ID
                                                                                .ne(
                                                                                    workflow
                                                                                        .id()))))
                                                        .fetchOptional()
                                                        .map(Record1::value1)
                                                        .orElse(0)
                                                    > 0) {
                                                  final var oldLiveness =
                                                      liveness.remove(workflowRunId);
                                                  if (oldLiveness != null) {
                                                    final var oldLivenessLock = oldLiveness.get();
                                                    if (oldLivenessLock != null) {
                                                      oldLivenessLock.set(false);
                                                    }
                                                  }
                                                  final var dbWorkflow =
                                                      DatabaseWorkflow.reinitialise(
                                                          target,
                                                          workflowRunId,
                                                          workflow.id(),
                                                          name,
                                                          version,
                                                          candidateId,
                                                          arguments,
                                                          engineParameters,
                                                          metadata,
                                                          externalIds,
                                                          liveness(workflowRunId),
                                                          externalKeys,
                                                          consumableResources,
                                                          transaction);
                                                  return handler.reinitialise(
                                                      candidateId,
                                                      new ConsumableResourceChecker(
                                                          target,
                                                          dataSource,
                                                          executor(),
                                                          dbWorkflow.dbId(),
                                                          liveness(dbWorkflow.dbId()),
                                                          name,
                                                          version,
                                                          candidateId,
                                                          consumableResources,
                                                          new Runnable() {
                                                            private boolean launched;

                                                            @Override
                                                            public void run() {
                                                              if (launched) {
                                                                throw new IllegalStateException(
                                                                    "Workflow has already been"
                                                                        + " launched");
                                                              }
                                                              launched = true;
                                                              startTransaction(
                                                                  runTransaction ->
                                                                      DatabaseBackedProcessor.this
                                                                          .start( // runs when
                                                                              // action
                                                                              // VIDARR-REATTEMPT'd
                                                                              target,
                                                                              workflow.definition(),
                                                                              dbWorkflow,
                                                                              runTransaction));
                                                            }
                                                          }));
                                                } else {
                                                  return handler.matchExisting(
                                                      candidates.get(0).second());
                                                }
                                              } else {
                                                return handler.multipleMatches(
                                                    candidates.stream()
                                                        .map(Pair::second)
                                                        .collect(Collectors.toList()));
                                              }
                                            } catch (SQLException e) {
                                              return handler.internalError(e);
                                            }
                                          })
                                      .orElseGet(() -> handler.unknownWorkflow(name, version));
                                } catch (SQLException e) {
                                  return handler.internalError(e);
                                }
                              });
                  connection.commit();
                  return submitResult;
                }
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            })
        .orElseGet(() -> handler.unknownTarget(targetName));
  }

  protected abstract Optional<Target> targetByName(String name);

  int updateVersions(BulkVersionRequest request) {
    final var counter = new AtomicInteger();
    startTransaction(
        context -> {
          for (final var update : request.getUpdates()) {

            counter.addAndGet(
                context
                    .insertInto(EXTERNAL_ID_VERSION)
                    .columns(
                        EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                        EXTERNAL_ID_VERSION.KEY,
                        EXTERNAL_ID_VERSION.VALUE)
                    .select(
                        DSL.selectDistinct(
                                EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                                DSL.val(request.getNewVersionKey()),
                                DSL.val(update.getAdd()))
                            .from(
                                EXTERNAL_ID_VERSION
                                    .join(EXTERNAL_ID)
                                    .on(
                                        EXTERNAL_ID_VERSION.EXTERNAL_ID_ID.eq(
                                            EXTERNAL_ID_VERSION.EXTERNAL_ID_ID)))
                            .where(
                                EXTERNAL_ID
                                    .PROVIDER
                                    .eq(request.getProvider())
                                    .and(EXTERNAL_ID.EXTERNAL_ID_.eq(update.getId()))
                                    .and(EXTERNAL_ID_VERSION.KEY.eq(request.getOldVersionKey()))
                                    .and(EXTERNAL_ID_VERSION.VALUE.eq(update.getOld()))))
                    .onConflictDoNothing()
                    .execute());
          }
        });
    return counter.get();
  }

  private Set<String> validateWorkflowInputs(
      ObjectNode labels,
      JsonNode arguments,
      JsonNode engineParameters,
      JsonNode metadata,
      Map<String, JsonNode> consumableResources,
      Target target,
      WorkflowInformation workflow) {
    return Stream.of(
            validateInput(
                MAPPER, target, workflow.definition(), arguments, metadata, engineParameters),
            workflow.validateLabels(labels),
            target
                .consumableResources()
                .flatMap(r -> r.second().inputFromSubmitter().stream())
                .flatMap(cr -> checkConsumableResource(consumableResources, cr)))
        .flatMap(Function.identity())
        .collect(Collectors.toSet());
  }

  private Optional<String> validateWorkflowRetry(JsonNode arguments, WorkflowInformation workflow) {
    final var retryCounts =
        workflow
            .definition()
            .parameters()
            .flatMap(
                p ->
                    arguments.has(p.name())
                        ? p.type().apply(new ExtractRetryValues(MAPPER, arguments.get(p.name())))
                        : Stream.empty())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    return retryCounts.keySet().stream()
        .max(Comparator.naturalOrder())
        .flatMap(
            max -> {
              if (IntStream.rangeClosed(0, max).allMatch(retryCounts::containsKey)) {
                return Optional.empty();
              } else {
                return Optional.of(
                    String.format("Retry keys are not a continuous range from 0 to %d.", max));
              }
            })
        .or(
            () ->
                retryCounts.values().stream()
                    .max(Comparator.naturalOrder())
                    .flatMap(
                        max -> {
                          final var badEntries =
                              retryCounts.entrySet().stream()
                                  .filter(e -> !e.getValue().equals(max))
                                  .map(Map.Entry::getKey)
                                  .collect(Collectors.toList());
                          if (badEntries.isEmpty()) {
                            return Optional.empty();
                          } else {
                            return Optional.of(
                                "Retry values are not consistent. Some entries are missing for: "
                                    + badEntries);
                          }
                        }));
  }

  private static String hashFromAnalysisId(String id) {
    Matcher matcher = BaseProcessor.ANALYSIS_RECORD_ID.matcher(id);
    if (!matcher.matches())
      throw new IllegalArgumentException(
          String.format("'%s' is a malformed Vidarr file identifier", id));
    return matcher.group("hash");
  }
}
