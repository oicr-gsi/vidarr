package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.ACTIVE_OPERATION;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.ACTIVE_WORKFLOW_RUN;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.ANALYSIS;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.ANALYSIS_EXTERNAL_ID;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.EXTERNAL_ID;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.EXTERNAL_ID_VERSION;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_DEFINITION;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_RUN;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_VERSION;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_VERSION_ACCESSORY;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.jooq.impl.DSL.trueCondition;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.OperationStatus;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition.Output;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition.Parameter;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.api.BulkVersionRequest;
import ca.on.oicr.gsi.vidarr.api.BulkVersionUpdate;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ExternalMultiVersionKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.core.BaseProcessor;
import ca.on.oicr.gsi.vidarr.core.CheckOutputCompatibility;
import ca.on.oicr.gsi.vidarr.core.ExtractInputExternalIds;
import ca.on.oicr.gsi.vidarr.core.ExtractInputVidarrIds;
import ca.on.oicr.gsi.vidarr.core.ExtractOutputKeys;
import ca.on.oicr.gsi.vidarr.core.ExtractRetryValues;
import ca.on.oicr.gsi.vidarr.core.FileMetadata;
import ca.on.oicr.gsi.vidarr.core.NoOpWorkflowEngine;
import ca.on.oicr.gsi.vidarr.core.OutputCompatibility;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;
import ca.on.oicr.gsi.vidarr.core.RecoveryType;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.core.ValidateJsonToSimpleType;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.records.ExternalIdVersionRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.zaxxer.hikari.HikariDataSource;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.undertow.util.StatusCodes;
import java.io.IOError;
import java.lang.ref.SoftReference;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep3;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Result;
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

    T externalIdMismatch(String error);

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
  // Jdk8Module is a compatibility fix for de/serializing Optionals
  static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
  public static final TypeReference<Map<String, OutputType>> OUTPUT_JSON_TYPE =
      new TypeReference<>() {};
  public static final TypeReference<Map<String, InputType>> PARAMETER_JSON_TYPE =
      new TypeReference<>() {};

  private static WorkflowDefinition buildDefinitionFromRecord(
      DSLContext context, Record record) {
    final Map<String, String> accessoryFiles =
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
            .map(e -> new Parameter(e.getValue(), e.getKey())),
        MAPPER
            .convertValue(record.get(WORKFLOW_VERSION.METADATA), OUTPUT_JSON_TYPE)
            .entrySet()
            .stream()
            .map(e -> new Output(e.getValue(), e.getKey())));
  }

  private static Stream<String> checkConsumableResource(
      Map<String, JsonNode> consumableResources, Pair<String, BasicType> resource) {
    if (consumableResources == null) {
      return Stream.of("Missing consumableResources attribute");
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
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(name.getBytes(StandardCharsets.UTF_8));
      for (final String id : inputIds) {
        final byte[] idBytes = hashFromAnalysisId(id).getBytes(StandardCharsets.UTF_8);
        digest.update(new byte[] {0});
        digest.update(idBytes);
      }
      final ArrayList<? extends ExternalId> sortedExternalIds = new ArrayList<>(externalIds);
      sortedExternalIds.sort(
          Comparator.comparing(ExternalId::getProvider).thenComparing(ExternalId::getId));
      for (final ExternalId id : sortedExternalIds) {
        digest.update(new byte[] {0});
        digest.update(new byte[] {0});
        digest.update(id.getProvider().getBytes(StandardCharsets.UTF_8));
        digest.update(new byte[] {0});
        digest.update(id.getId().getBytes(StandardCharsets.UTF_8));
        digest.update(new byte[] {0});
      }

      // The client may submit any number of workflow labels, but this hashing/matching only
      // takes into account the labels that the workflow is configured with.
      for (final String label : labelsFromWorkflow) {
        digest.update(new byte[] {0});
        digest.update(label.getBytes(StandardCharsets.UTF_8));
        digest.update(new byte[] {0});
        digest.update(MAPPER.writeValueAsBytes(labelsFromClient.get(label)));
        // Note that if the .get(label) value is a string, writeValueAsBytes will also encode the
        // literal (escaped) quote marks around the value.
        // This means that if you are generating a workflow run hash outside of vidarr, if your
        // label value is a string then you need to surround it with literal (escaped) quotes when
        // hashing
      }

      return hexDigits(digest.digest());
    } catch (NoSuchAlgorithmException | JsonProcessingException e) {
      throw new IOError(e);
    }
  }

  /**
   * Sanity check an individual workflow run's labels against the workflow definition's labels
   *
   * @param providedLabels labels from an individual workflow run
   * @param expectedLabels labels from the workflow definition
   * @return Stream of label errors
   */
  public static Stream<String> validateLabels(
      ObjectNode providedLabels, Map<String, BasicType> expectedLabels) {

    // Provided and Expected labels should have the same counts
    // If no labels provided nor expected, return early
    final int providedCount = providedLabels == null ? 0 : providedLabels.size();
    final int expectedLabelCount = expectedLabels == null ? 0 : expectedLabels.size();
    if (expectedLabelCount == 0 && providedCount == 0) {
      return Stream.empty();
    }
    if (providedCount != expectedLabelCount) {
      return Stream.of(
          String.format(
              "%d labels are provided but %d are expected.", providedCount, expectedLabelCount));
    }

    // For every expected label, see if that label is in the provided labels
    // If it is, validate that the label can be resolved to a vidarr type and return it
    // Otherwise report label not provided
    return expectedLabels.entrySet().stream()
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
    InsertValuesStep3<ExternalIdVersionRecord, Integer, String, String> externalVersionInsert =
        transaction
            .insertInto(EXTERNAL_ID_VERSION)
            .columns(
                EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                EXTERNAL_ID_VERSION.KEY,
                EXTERNAL_ID_VERSION.VALUE);
    for (final ExternalKey externalKey : externalKeys) {
      final List<String> matchKeys =
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
      for (final Entry<String, String> entry : externalKey.getVersions().entrySet()) {
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
    try (final Connection connection = dataSource.getConnection()) {
      return DSL.using(connection, SQLDialect.POSTGRES)
          .transactionResult(
              context -> {
                final DSLContext transaction = DSL.using(context);
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
                            final SoftReference<AtomicBoolean> oldLiveness =
                                liveness.remove(id_and_dead.component1());
                            if (oldLiveness != null) {
                              final AtomicBoolean oldLivenessLock = oldLiveness.get();
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
                                  final Optional<FileMetadata> result = pathForId(id);
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
      MaxInFlightByWorkflow maxInFlightByWorkflow,
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
    final DatabaseWorkflow dbWorkflow =
        DatabaseWorkflow.createNew(
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
    InsertValuesStep3<ExternalIdVersionRecord, Integer, String, String> externalVersionInsert =
        transaction
            .insertInto(EXTERNAL_ID_VERSION)
            .columns(
                EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                EXTERNAL_ID_VERSION.KEY,
                EXTERNAL_ID_VERSION.VALUE);
    for (final ExternalKey externalKey : externalKeys) {
      for (final Entry<String, String> entry : externalKey.getVersions().entrySet()) {
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
            maxInFlightByWorkflow,
            name,
            version,
            candidateId,
            consumableResources,
            dbWorkflow.created(),
            new Runnable() {
              private boolean launched;

              @Override
              public void run() {
                if (launched) {
                  throw new IllegalStateException("Workflow has already been" + " launched");
                }
                launched = true;
                inTransaction(
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

  final void recover(Consumer<Runnable> startRaw, MaxInFlightByWorkflow maxInflightByWorkflow,
      Map<String, OutputProvisioner<?, ?>> outputProvisioners,
      Map<String, Semaphore> reprovisionCounter)
      throws SQLException {
    try (final Connection connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              context -> {
                DSLContext dsl = DSL.using(context);
                Map<Long, List<DatabaseOperation>> operations =
                    dsl
                        .select(ACTIVE_OPERATION.asterisk())
                        .from(ACTIVE_OPERATION)
                        .where(
                            ACTIVE_OPERATION
                                .STATUS
                                .eq(OperationStatus.FAILED)
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
                                collectingAndThen(
                                    toList(),
                                    ops -> {
                                      Phase maxEnginePhase =
                                          ops.stream()
                                              .map(r -> r.get(ACTIVE_OPERATION.ENGINE_PHASE))
                                              .filter(Objects::nonNull)
                                              .max(Comparator.comparing(Phase::ordinal))
                                              .orElse(null);
                                      // all operations are required to have an engine phase so this
                                      // will never be null

                                      // select the operations with max engine phase as recovery
                                      // only operates on one phase
                                      return ops.stream()
                                          .filter(
                                              r ->
                                                  r.get(ACTIVE_OPERATION.ENGINE_PHASE)
                                                      .equals(maxEnginePhase))
                                          .map(
                                              r ->
                                                  DatabaseOperation.recover(
                                                      r,
                                                      liveness(
                                                          r.get(ACTIVE_OPERATION.WORKFLOW_RUN_ID))))
                                          .collect(toList());
                                    })));
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
                        record -> {
                          if (record.get(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE).equals(Phase.REPROVISION)) {
                            try{
                            System.err.printf("Recovering reprovisioning task for %s...%n", record.get(WORKFLOW_RUN.HASH_ID));

                              Semaphore s = reprovisionCounter.getOrDefault(record.get(WORKFLOW_RUN.HASH_ID), new Semaphore(1));
                              if(!s.tryAcquire()){
                                throw new Exception("There is already a reprovision request on this workflow run right now. Please try again later.");
                              }
                              reprovisionCounter.put(record.get(WORKFLOW_RUN.HASH_ID), s);
                            final List<DatabaseOperation> activeOperations =
                                operations.getOrDefault(
                                    record.get(ACTIVE_WORKFLOW_RUN.ID), List.of());
                            if (activeOperations.isEmpty()){
                              throw new Exception(String.format("Workflow run %s does not have any active operations, insufficient information for recovery.", record.get(WORKFLOW_RUN.HASH_ID)));
                            }

                            // Get recovery state back
                            JsonNode recoveryState = activeOperations.get(0).recoveryState().get("state").get("metadata");

                            if(!recoveryState.has("outputReprovisioner")){
                              throw new Exception(
                                  String.format("Reprovisioning for %s lacks outputReprovisioner metadata field.",
                                      record.get(WORKFLOW_RUN.HASH_ID)
                                  ));
                            }

                            // Recreate Target with the output provisioner name
                            OutputProvisioner<?, ?> outputReprovisioner = outputProvisioners.get(
                                recoveryState.get("outputReprovisioner").textValue());
                            if(null == outputReprovisioner){
                              throw new Exception(String.format("outputReprovisioner %s is not represented in config.",
                                  recoveryState.get("outputReprovisioner").textValue()));
                            }
                            Target newTarget = makeReprovisionTarget(outputReprovisioner);

                            final DatabaseWorkflow workflow =
                                DatabaseWorkflow.recover(
                                    newTarget,
                                    record,
                                    liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                    dsl);

                            for (final DatabaseOperation operation :
                                activeOperations) {
                              operation.linkTo(workflow);
                            }

                            recover(
                                newTarget,
                                buildDefinitionFromRecord(context.dsl(), record),
                                workflow,
                                activeOperations,
                                RecoveryType.RECOVER);
                              reprovisionCounter.get(record.get(WORKFLOW_RUN.HASH_ID)).release();
                            } catch (Exception e) {
                              String erroneousHash = record.get(WORKFLOW_RUN.HASH_ID);
                              System.err.printf(
                                  "Error recovering reprovisioning %s: \n", erroneousHash);
                              e.printStackTrace();
                              BadRecoveryTracker.add(erroneousHash);
                              System.err.println(
                                  "Continuing recovery on next record in database if one exists.");
                            }
                          } else {
                            targetByName(record.get(ACTIVE_WORKFLOW_RUN.TARGET))
                                .ifPresent(
                                    target -> {
                                      try {
                                        if (record.get(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE)
                                            == Phase.WAITING_FOR_RESOURCES) {
                                          final WorkflowDefinition definition =
                                              buildDefinitionFromRecord(context.dsl(), record);
                                          final List<DatabaseOperation> activeOperations =
                                              operations.getOrDefault(
                                                  record.get(ACTIVE_WORKFLOW_RUN.ID), List.of());
                                          final DatabaseWorkflow workflow =
                                              DatabaseWorkflow.recover(
                                                  target,
                                                  record,
                                                  liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                  dsl);
                                          for (final DatabaseOperation operation :
                                              activeOperations) {
                                            operation.linkTo(workflow);
                                          }
                                          final Map<String, JsonNode> consumableResources =
                                              MAPPER.convertValue(
                                                  record.get(
                                                      ACTIVE_WORKFLOW_RUN.CONSUMABLE_RESOURCES),
                                                  new TypeReference<>() {});
                                          startRaw.accept(
                                              new ConsumableResourceChecker(
                                                  target,
                                                  dataSource,
                                                  executor(),
                                                  record.get(ACTIVE_WORKFLOW_RUN.ID),
                                                  liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                  maxInflightByWorkflow,
                                                  record.get(WORKFLOW.NAME),
                                                  record.get(WORKFLOW_VERSION.VERSION),
                                                  record.get(WORKFLOW_RUN.HASH_ID),
                                                  consumableResources,
                                                  record.get(WORKFLOW_RUN.CREATED).toInstant(),
                                                  () ->
                                                      recover(
                                                          target,
                                                          definition,
                                                          workflow,
                                                          activeOperations,
                                                          RecoveryType.RECOVER)));
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
                                                              Optional.ofNullable(
                                                                  record.get(
                                                                      ACTIVE_WORKFLOW_RUN
                                                                          .CONSUMABLE_RESOURCES))));
                                          final DatabaseWorkflow workflow =
                                              DatabaseWorkflow.recover(
                                                  target,
                                                  record,
                                                  liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                  dsl);
                                          final List<DatabaseOperation> activeOperations =
                                              operations.getOrDefault(
                                                  record.get(ACTIVE_WORKFLOW_RUN.ID), List.of());
                                          for (final DatabaseOperation operation :
                                              activeOperations) {
                                            operation.linkTo(workflow);
                                          }
                                          recover(
                                              target,
                                              buildDefinitionFromRecord(context.dsl(), record),
                                              workflow,
                                              activeOperations,
                                              RecoveryType.RECOVER);
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
                                    }
                                );
                          }
                        });
              });
      connection.commit();
    }
  }

  public final List<String> retry(Optional<List<String>> workflowRunIds) throws SQLException {
    try (final Connection connection = dataSource.getConnection()) {
      final ArrayList<String> ids = new ArrayList<>();
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              context -> {
                DSLContext dsl = DSL.using(context);
                Map<Long, List<DatabaseOperation>> operations =
                    dsl
                        .select(ACTIVE_OPERATION.asterisk())
                        .from(
                            ACTIVE_OPERATION
                                .join(ACTIVE_WORKFLOW_RUN)
                                .on(ACTIVE_OPERATION.WORKFLOW_RUN_ID.eq(ACTIVE_WORKFLOW_RUN.ID))
                                .join(WORKFLOW_RUN)
                                .on(WORKFLOW_RUN.ID.eq(ACTIVE_WORKFLOW_RUN.ID)))
                        .where(
                            ACTIVE_OPERATION
                                .STATUS
                                .eq(OperationStatus.FAILED)
                                .and(ACTIVE_OPERATION.ENGINE_PHASE.eq(Phase.PROVISION_OUT))
                                .and(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE.eq(Phase.FAILED))
                                .and(ACTIVE_OPERATION.ATTEMPT.eq(ACTIVE_WORKFLOW_RUN.ATTEMPT))
                                .and(
                                    workflowRunIds
                                        .map(WORKFLOW_RUN.HASH_ID::in)
                                        .orElse(trueCondition())))
                        .forUpdate()
                        .stream()
                        .collect(
                            Collectors.groupingBy(
                                r -> r.get(ACTIVE_OPERATION.WORKFLOW_RUN_ID),
                                Collectors.mapping(
                                    r -> {
                                      r.set(ACTIVE_OPERATION.STATUS, OperationStatus.INITIALIZING);
                                      return DatabaseOperation.recover(
                                          r, liveness(r.get(ACTIVE_OPERATION.WORKFLOW_RUN_ID)));
                                    },
                                    toList())));
                final int updated =
                    dsl.update(ACTIVE_WORKFLOW_RUN)
                        .set(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE, Phase.PROVISION_OUT)
                        .where(ACTIVE_WORKFLOW_RUN.ID.in(operations.keySet()))
                        .execute();
                if (updated != operations.size()) {
                  System.err.printf(
                      "Attempting to retry updated %d workflow runs, but should have updated %d.",
                      updated, operations.size());
                }
                dsl.select()
                    .from(
                        ACTIVE_WORKFLOW_RUN
                            .join(WORKFLOW_RUN)
                            .on(ACTIVE_WORKFLOW_RUN.ID.eq(WORKFLOW_RUN.ID))
                            .join(WORKFLOW_VERSION)
                            .on(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID))
                            .join(WORKFLOW_DEFINITION)
                            .on(WORKFLOW_VERSION.WORKFLOW_DEFINITION.eq(WORKFLOW_DEFINITION.ID)))
                    .where(ACTIVE_WORKFLOW_RUN.ID.in(operations.keySet()))
                    .forUpdate()
                    .forEach(
                        record ->
                            targetByName(record.get(ACTIVE_WORKFLOW_RUN.TARGET))
                                .ifPresent(
                                    target -> {
                                      try {
                                        ids.add(record.get(WORKFLOW_RUN.HASH_ID));
                                        System.err.printf(
                                            "Retrying workflow %s...\n",
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
                                                            Optional.ofNullable(
                                                                record.get(
                                                                    ACTIVE_WORKFLOW_RUN
                                                                        .CONSUMABLE_RESOURCES))));
                                        final List<DatabaseOperation> activeOperations =
                                            operations.get(record.get(ACTIVE_WORKFLOW_RUN.ID));
                                        if (activeOperations.isEmpty()) {
                                          System.err.printf(
                                              "Error retrying workflow run %s: no operations match\n",
                                              record.get(WORKFLOW_RUN.HASH_ID));
                                          return;
                                        }
                                        final DatabaseWorkflow workflow =
                                            DatabaseWorkflow.recover(
                                                target,
                                                record,
                                                liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                dsl);
                                        for (final DatabaseOperation operation : activeOperations) {
                                          operation.linkTo(workflow);
                                        }
                                        recover(
                                            target,
                                            buildDefinitionFromRecord(context.dsl(), record),
                                            workflow,
                                            activeOperations,
                                            RecoveryType.RETRY);
                                      } catch (Exception e) {
                                        String erroneousHash = record.get(WORKFLOW_RUN.HASH_ID);
                                        System.err.printf(
                                            "Error retrying workflow run %s: \n", erroneousHash);
                                        e.printStackTrace();
                                        BadRecoveryTracker.add(erroneousHash);
                                        System.err.println(
                                            "Continuing recovery on next record in database if one exists.");
                                      }
                                    }));
              });
      connection.commit();
      return ids;
    }
  }

  protected final Set<String> recoveryFailures() {
    return BadRecoveryTracker.badRecoveryIds;
  }

  private Target makeReprovisionTarget(OutputProvisioner<?, ?> provisioner){
    return new Target() {
      @Override
      public Stream<Pair<String, ConsumableResource>> consumableResources() {
        return Stream.empty();
      }

      @Override
      public WorkflowEngine<?, ?> engine() {
        return new NoOpWorkflowEngine();
      }

      @Override
      public InputProvisioner<?> provisionerFor(InputProvisionFormat type) {
        return new RawInputProvisioner();
      }

      @Override
      public OutputProvisioner<?, ?> provisionerFor(OutputProvisionFormat type) {
        return provisioner;
      }

      @Override
      public Stream<RuntimeProvisioner<?>> runtimeProvisioners() {
        return Stream.empty();
      }
    };
  }

  public <T> T reprovisionOut(String workflowRunId,
      String provisionerName,
      OutputProvisioner<?, ?> provisioner,
      String outputPath,
      SubmissionResultHandler<T> handler) {
    AtomicReference<T> ret = new AtomicReference<>();
    // New target to aim the new job at
    Target newTarget = makeReprovisionTarget(provisioner);

    // Create a new active_workflow_run from the existing and finished workflow_run
    try {
      try (final Connection connection = dataSource.getConnection()) {
        DSL.using(connection, SQLDialect.POSTGRES)
            .transaction(
                context -> {
                  DSLContext dsl = DSL.using(context);
                  Result<Record> result = dsl.select()
                      .from(
                          WORKFLOW_RUN
                              .join(WORKFLOW_VERSION)
                              .on(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID)))
                      .where(WORKFLOW_RUN.HASH_ID.eq(workflowRunId)
                          .and(WORKFLOW_RUN.ID.notIn(
                              dsl.select(ACTIVE_WORKFLOW_RUN.ID)
                                  .from(ACTIVE_WORKFLOW_RUN))))
                      .fetch();
                  if (result.isNotEmpty() && result.size() == 1) {
                    // Populate the workflow run metadata with information will we need
                    // for reprovisioning or recovery
                    Record record = result.get(0);
                    JsonNode metadata = record.get(WORKFLOW_RUN.METADATA);
                    OffsetDateTime originalCompleted = record.get(WORKFLOW_RUN.COMPLETED);
                    Iterator<Entry<String, JsonNode>> iterator = metadata.fields();
                    while (iterator.hasNext()) {
                      Entry<String, JsonNode> entry = iterator.next();
                      ArrayNode contents = (ArrayNode) entry.getValue().get("contents");
                      Iterator<JsonNode> iterator2 = contents.elements();
                      while (iterator2.hasNext()) {
                        ObjectNode content = (ObjectNode) iterator2.next();
                        if (content.has("outputDirectory")) {
                          content.set("originalDirectory", content.get("outputDirectory"));
                          content.put("outputDirectory", outputPath);
                          content.put("outputReprovisioner", provisionerName);
                          content.put("originalCompleted", originalCompleted.toInstant().getEpochSecond());
                          content.put("originalCompletedOffset", originalCompleted.getOffset().toString());
                        } // else there's some other kind of content here, maybe the next one
                      }
                    }

                    // Null out Completed time in database if it wasn't already
                    dsl.update(WORKFLOW_RUN)
                        .setNull(WORKFLOW_RUN.COMPLETED)
                        .set(WORKFLOW_RUN.METADATA, metadata)
                        .where(WORKFLOW_RUN.HASH_ID.eq(record.get(WORKFLOW_RUN.HASH_ID)))
                        .execute();

                    // Get workflow definition needed downstream
                    Optional<WorkflowInformation> definition;
                    try {
                      definition = getWorkflowByName(
                          record.get(WORKFLOW_VERSION.NAME),
                          record.get(WORKFLOW_VERSION.VERSION), dsl);
                    } catch (SQLException e) {
                      throw new RuntimeException(e);
                    }

                    // Get the analysis, and also the external ids for those analysis records
                    Map<ProvenanceAnalysisRecord<ExternalId>, JsonNode> analysis = new HashMap<>();
                    Map<Integer, Set<ExternalId>> externalIdsByAnalysis = new HashMap<>();
                    dsl.select()
                        .from(ANALYSIS)
                        .where(ANALYSIS.WORKFLOW_RUN_ID.eq(record.get(WORKFLOW_RUN.ID)))

                        // For every analysis record associated with this workflow run, create
                        // an object for use downstream
                        .forEach(analysisDbRecord -> {
                          ProvenanceAnalysisRecord<ExternalId> analysisObject = new ProvenanceAnalysisRecord<>();
                          analysisObject.setId(analysisDbRecord.get(ANALYSIS.HASH_ID));
                          analysisObject.setType(analysisDbRecord.get(ANALYSIS.ANALYSIS_TYPE));
                          analysisObject.setChecksum(analysisDbRecord.get(ANALYSIS.FILE_CHECKSUM));
                          analysisObject.setChecksumType(analysisDbRecord.get(ANALYSIS.FILE_CHECKSUM_TYPE));
                          analysisObject.setMetatype(analysisDbRecord.get(ANALYSIS.FILE_METATYPE));
                          analysisObject.setPath(analysisDbRecord.get(ANALYSIS.FILE_PATH));
                          analysisObject.setSize(analysisDbRecord.get(ANALYSIS.FILE_SIZE));

                          analysisObject.setLabels(
                              mapper().convertValue(new PostgresJSONBBinding().converter()
                                  .from(analysisDbRecord.get(ANALYSIS.LABELS)), new TypeReference<>() {}));
                          analysisObject.setWorkflowRun(record.get(WORKFLOW_RUN.HASH_ID));
                          analysisObject.setCreated(analysisDbRecord.get(ANALYSIS.CREATED).toZonedDateTime());

                          // Get the external IDs associated with this analysis record
                          Integer analysisId = analysisDbRecord.get(ANALYSIS.ID);
                          dsl.select()
                              .from(EXTERNAL_ID)
                              .join(ANALYSIS_EXTERNAL_ID)
                              .on(EXTERNAL_ID.ID.eq(ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID))
                              .where(ANALYSIS_EXTERNAL_ID.ANALYSIS_ID.eq(analysisId))

                              // Build a set of external ids and associate it to the analysis id
                              .forEach(externalIdDbRecord -> {
                                    Set<ExternalId> externalIdsForAnalysisRecord =
                                        externalIdsByAnalysis.containsKey(analysisId) ? externalIdsByAnalysis.get(
                                            analysisId)
                                            : new HashSet<>();
                                    externalIdsForAnalysisRecord.add(new ExternalId(externalIdDbRecord.get(EXTERNAL_ID.PROVIDER),
                                        externalIdDbRecord.get(EXTERNAL_ID.EXTERNAL_ID_)));
                                    externalIdsByAnalysis.put(analysisId, externalIdsForAnalysisRecord);
                                  }
                              );
                          analysisObject.setExternalKeys(
                              externalIdsByAnalysis.get(analysisId).stream().toList());

                          // Workflow run metadata is split up by analysis file, but we cannot
                          // get them by name because that information is lost. We must use the
                          // path instead.
                          // Check that the workflow run metadata's `originalDirectory` corresponds
                          // to the paths already set on the analysis object, then associate
                          // just that part of the metadata
                          JsonNode workflowRunMetadataPart = null;
                          var metadataIterator = metadata.iterator();
                          while (metadataIterator.hasNext()) {
                            var metadatum = metadataIterator.next();
                            ArrayNode contents = (ArrayNode) metadatum.get("contents");
                            var contentsIterator = contents.elements();
                            while (contentsIterator.hasNext()) {
                              var content = contentsIterator.next();
                              if (content.has("originalDirectory") && analysisObject.getPath()
                                  .startsWith(content.get(
                                      "originalDirectory").textValue())) {
                                workflowRunMetadataPart = content;
                                break;
                              }
                            }
                          }
                          analysis.put(analysisObject, workflowRunMetadataPart);
                        });

                    try {
                      final DatabaseWorkflow dbWorkflow = DatabaseWorkflow.createActive(
                          "reprovision",
                          newTarget,
                          record.get(WORKFLOW_RUN.ID),
                          "reprovision", //record.get(WORKFLOW_VERSION.NAME),
                          "1", // record.get(WORKFLOW_VERSION.VERSION),
                          record.get(WORKFLOW_RUN.HASH_ID),
                          record.get(WORKFLOW_RUN.ARGUMENTS),
                          record.get(WORKFLOW_RUN.ENGINE_PARAMETERS),
                          metadata,
                          externalIdsByAnalysis.values().stream().flatMap(Collection::stream).collect(
                              Collectors.toSet()),
                          Map.of(), //empty consumable resources
                          record.get(WORKFLOW_RUN.CREATED).toInstant(),
                          this::liveness,
                          dsl,
                          Phase.REPROVISION
                      );

                      ret.set(handler.launched(record.get(WORKFLOW_RUN.HASH_ID),
                          new ConsumableResourceChecker(
                              newTarget,
                              dataSource,
                              executor(),
                              dbWorkflow.dbId(),
                              liveness(dbWorkflow.dbId()),
                              new MaxInFlightByWorkflow(),
                              "reprovision",
                              "1",
                              record.get(WORKFLOW_RUN.HASH_ID),
                              Map.of(),
                              record.get(WORKFLOW_RUN.CREATED).toInstant(),
                              new Runnable() {
                                private boolean launched;

                                @Override
                                public void run() {
                                  if (launched) {
                                    throw new IllegalStateException(
                                        "Workflow has already been" + " launched");
                                  }
                                  launched = true;
                                  inTransaction(
                                      runTransaction ->
                                          DatabaseBackedProcessor.this.reprovision(
                                              newTarget, definition.get().definition(),
                                              dbWorkflow, analysis, originalCompleted,
                                              runTransaction));
                                }
                              })));
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  } else if (result.size() > 1){
                    ret.set(handler.multipleMatches(
                        result.stream()
                            .map(r -> r.get(WORKFLOW_RUN.HASH_ID))
                            .toList()));
                  } else { // size == 0
                    ret.set(handler.invalidWorkflow(
                        Set.of(
                            String.format(
                                "No record for workflow run hash id %s%n",
                                workflowRunId))));
                  }
                });
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return ret.get();
  }

  protected final Optional<FileMetadata> resolveInDatabase(String inputId) {
    try {
      try (final Connection connection = dataSource.getConnection()) {
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
                              .toList();
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
  public final void inTransaction(Consumer<DSLContext> operation) {
    databaseLock.acquireUninterruptibly();
    try {
      try (final Connection connection = dataSource.getConnection()) {
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
      MaxInFlightByWorkflow maxInFlightByWorkflow,
      SubmissionResultHandler<T> handler) {
    return targetByName(targetName)
        .map(
            target -> {
              try {
                try (final Connection connection = dataSource.getConnection()) {
                  final T submitResult =
                      DSL.using(connection, SQLDialect.POSTGRES)
                          .transactionResult(
                              context -> {
                                final DSLContext transaction = DSL.using(context);
                                try {
                                  return getWorkflowByName(name, version, transaction)
                                      .map(
                                          workflow -> {
                                            final Set<String> errors =
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
                                            final Optional<String> retryError =
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

                                            final TreeSet<String> inputIds =
                                                extractWorkflowInputIds(arguments, workflow);

                                            final TreeSet<String> unresolvedIds = new TreeSet<>();

                                            final TreeSet<ExternalId> externalIds =
                                                extractExternalIds(
                                                    arguments, workflow, unresolvedIds);
                                            if (!unresolvedIds.isEmpty()) {
                                              return handler.unresolvedIds(unresolvedIds);
                                            }
                                            if (externalKeys.stream()
                                                .anyMatch(k -> k.getVersions().isEmpty())) {
                                              return handler.missingExternalIdVersion();
                                            }

                                            final Set<Pair<String, String>> externalKeyIds =
                                                externalKeys.stream()
                                                    .map(
                                                        k -> new Pair<>(k.getProvider(), k.getId()))
                                                    .collect(Collectors.toSet());

                                            final Set<Pair<String, String>> requiredOutputKeys =
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
                                            final Set<Pair<String, String>> optionalOutputKeys =
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

                                            if (externalIds.size() != externalKeys.size()) {
                                              return handler.externalIdMismatch(
                                                  String.format(
                                                      "%d External IDs found (%s) but %d External Keys found (%s)!",
                                                      externalIds.size(),
                                                      externalIds,
                                                      externalKeys.size(),
                                                      externalKeys));
                                            }
                                            if (requiredOutputKeys.size() != externalKeys.size()) {
                                              return handler.externalIdMismatch(
                                                  String.format(
                                                      "%d required Output Keys found (%s) but %d External Keys found (%s)!",
                                                      requiredOutputKeys.size(),
                                                      requiredOutputKeys,
                                                      externalKeys.size(),
                                                      externalKeys));
                                            }
                                            if (!requiredOutputKeys.equals(externalKeyIds)) {
                                              return handler.externalIdMismatch(
                                                  String.format(
                                                      "Set of Required Output Keys (%s) does not match set of External Keys (%s)!",
                                                      requiredOutputKeys, externalKeys));
                                            }
                                            if (!externalKeyIds.containsAll(optionalOutputKeys)) {
                                              return handler.externalIdMismatch(
                                                  String.format(
                                                      "Set of External Key IDs (%s) does not contain all of the Optional Output Keys (%s)!",
                                                      externalKeyIds, optionalOutputKeys));
                                            }
                                            if (!externalKeyIds.equals(
                                                externalIds.stream()
                                                    .map(
                                                        k -> new Pair<>(k.getProvider(), k.getId()))
                                                    .collect(Collectors.toSet()))) {
                                              return handler.externalIdMismatch(
                                                  String.format(
                                                      "Unable to map External Key IDs (%s) to External ID by (Provider, ID): %s",
                                                      externalKeyIds, externalIds));
                                            }
                                            try {
                                              final String candidateId =
                                                  computeWorkflowRunHashId(
                                                      name,
                                                      labels,
                                                      workflow,
                                                      inputIds,
                                                      externalIds);
                                              final List<Candidate> candidates =
                                                  transaction
                                                      .select(
                                                          WORKFLOW_RUN.ID,
                                                          WORKFLOW_RUN.HASH_ID,
                                                          WORKFLOW_RUN.CREATED)
                                                      .from(WORKFLOW_RUN)
                                                      .where(WORKFLOW_RUN.HASH_ID.eq(candidateId))
                                                      .stream()
                                                      .map(
                                                          r ->
                                                              new Candidate(
                                                                  r.get(WORKFLOW_RUN.ID),
                                                                  r.get(WORKFLOW_RUN.HASH_ID),
                                                                  r.get(WORKFLOW_RUN.CREATED)
                                                                      .toInstant()))
                                                      .toList();
                                              if (candidates.isEmpty()) {
                                                if (handler.allowLaunch()) {
                                                  return launchNewWorkflowRun(
                                                      maxInFlightByWorkflow,
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
                                                final long workflowRunId = candidates.get(0).id();
                                                final HashMap<Pair<String, String>, List<String>>
                                                    knownMatches = new HashMap<>();
                                                final ArrayList<ExternalKey> missingKeys =
                                                    new ArrayList<>();
                                                for (final ExternalKey externalKey : externalKeys) {
                                                  final List<String> matchKeys =
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
                                                      candidates.get(0).workflowRun(), missingKeys);
                                                }

                                                // Exit early if no launching is to occur (e.g. dry
                                                // run or validate mode).
                                                if (!handler.allowLaunch()) {
                                                  return handler.matchExisting(
                                                      candidates.get(0).workflowRun());
                                                }

                                                addNewExternalKeyVersions(
                                                    externalKeys,
                                                    transaction,
                                                    workflowRunId,
                                                    knownMatches);

                                                updateLastAccessed(context.dsl(), candidateId);

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
                                                  final SoftReference<AtomicBoolean> oldLiveness =
                                                      liveness.remove(workflowRunId);
                                                  if (oldLiveness != null) {
                                                    final AtomicBoolean oldLivenessLock =
                                                        oldLiveness.get();
                                                    if (oldLivenessLock != null) {
                                                      oldLivenessLock.set(false);
                                                    }
                                                  }
                                                  final DatabaseWorkflow dbWorkflow =
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
                                                          candidates.get(0).created(),
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
                                                          maxInFlightByWorkflow,
                                                          name,
                                                          version,
                                                          candidateId,
                                                          consumableResources,
                                                          candidates.get(0).created(),
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
                                                              inTransaction(
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
                                                      candidates.get(0).workflowRun());
                                                }
                                              } else {
                                                return handler.multipleMatches(
                                                    candidates.stream()
                                                        .map(Candidate::workflowRun)
                                                        .toList());
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

  protected void updateLastAccessed(DSLContext context, String workflowRunHashId) {
    context
        .update(WORKFLOW_RUN)
        .set(WORKFLOW_RUN.LAST_ACCESSED, OffsetDateTime.now())
        .where(WORKFLOW_RUN.HASH_ID.eq(workflowRunHashId))
        .execute();
  }

  protected void updateLastAccessed(String workflowRunHashId) {
    inTransaction(context -> updateLastAccessed(context, workflowRunHashId));
  }

  int updateVersions(BulkVersionRequest request) {
    final AtomicInteger counter = new AtomicInteger();
    inTransaction(
        context -> {
          for (final BulkVersionUpdate update : request.getUpdates()) {

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
                .filter(r -> r.second().isInputFromSubmitterRequired())
                .flatMap(r -> r.second().inputFromSubmitter().stream())
                .flatMap(cr -> checkConsumableResource(consumableResources, cr)))
        .flatMap(Function.identity())
        .collect(Collectors.toSet());
  }

  private Optional<String> validateWorkflowRetry(JsonNode arguments, WorkflowInformation workflow) {
    final Map<Integer, Long> retryCounts =
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
                          final List<Integer> badEntries =
                              retryCounts.entrySet().stream()
                                  .filter(e -> !e.getValue().equals(max))
                                  .map(Entry::getKey)
                                  .toList();
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
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format("'%s' is a malformed Vidarr file identifier", id));
    }
    return matcher.group("hash");
  }

  public static String extractHashIfIsFullWorkflowRunId(String id) {
    Matcher matcher = BaseProcessor.WORKFLOW_RUN_ID.matcher(id);
    if (!matcher.matches()) {
      // assume it's already a hash
      return id;
    }
    return matcher.group("hash");
  }
}
