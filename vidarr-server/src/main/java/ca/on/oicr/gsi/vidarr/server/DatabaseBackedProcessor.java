package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.*;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.OutputProvisionType;
import ca.on.oicr.gsi.vidarr.SimpleType;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.core.*;
import ca.on.oicr.gsi.vidarr.server.dto.BulkVersionRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public abstract class DatabaseBackedProcessor
    extends BaseProcessor<DatabaseWorkflow, DatabaseOperation, DSLContext> {
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

  protected static final class WorkflowInformation {
    private final WorkflowDefinition definition;
    private final int id;
    private final Map<String, SimpleType> labels;

    private WorkflowInformation(
        int id, WorkflowDefinition definition, SortedMap<String, SimpleType> labels) {
      this.id = id;
      this.definition = definition;
      this.labels = labels;
    }

    public WorkflowDefinition definition() {
      return definition;
    }

    public void digestLabels(List<MessageDigest> digesters, ObjectNode providedLabels) {
      try {
        for (final var label : labels.keySet()) {
          final var labelBytes = label.getBytes(StandardCharsets.UTF_8);
          final var valueBytes = MAPPER.writeValueAsBytes(providedLabels.get(label));
          for (final var digester : digesters) {
            digester.update(new byte[] {0});
            digester.update(labelBytes);
            digester.update(new byte[] {0});
            digester.update(valueBytes);
          }
        }
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    public int id() {
      return id;
    }

    public Stream<String> validateLabels(ObjectNode providedLabels) {
      if (providedLabels.size() != labels.size()) {
        return Stream.of(
            String.format(
                "%d labels are provided by %d are expected.",
                providedLabels.size(), labels.size()));
      }
      return labels.entrySet().stream()
          .flatMap(
              entry -> {
                if (providedLabels.has(entry.getKey())) {
                  return entry
                      .getValue()
                      .apply(new CheckEngineType(providedLabels.get(entry.getKey())))
                      .map(String.format("Label %s: ", entry.getKey())::concat);
                } else {
                  return Stream.of(String.format("Label %s is not provided.", entry.getKey()));
                }
              });
    }
  }

  public static final TypeReference<SortedMap<String, SimpleType>> LABELS_JSON_TYPE =
      new TypeReference<>() {};
  static final ObjectMapper MAPPER = new ObjectMapper();
  public static final TypeReference<Map<String, OutputProvisionType>> OUTPUT_JSON_TYPE =
      new TypeReference<>() {};
  public static final TypeReference<Map<String, WorkflowConfiguration.Parameter>>
      PARAMETER_JSON_TYPE = new TypeReference<>() {};

  private static WorkflowDefinition buildDefinitionFromRecord(org.jooq.Record record) {
    return new WorkflowDefinition(
        record.get(WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE),
        record.get(WORKFLOW_VERSION.HASH_ID),
        record.get(WORKFLOW_DEFINITION.WORKFLOW_FILE),
        MAPPER
            .convertValue(record.get(WORKFLOW_VERSION.PARAMETERS), PARAMETER_JSON_TYPE)
            .entrySet()
            .stream()
            .map(
                e ->
                    new WorkflowDefinition.Parameter(
                        e.getValue().getType(), e.getKey(), e.getValue().isRequired())),
        MAPPER
            .convertValue(record.get(WORKFLOW_VERSION.METADATA), OUTPUT_JSON_TYPE)
            .entrySet()
            .stream()
            .map(e -> new WorkflowDefinition.Output(e.getValue(), e.getKey())));
  }

  private static String hashFromAnalysisId(String id) {
    return BaseProcessor.ANALYSIS_RECORD_ID.matcher(id).group("hash");
  }

  private final HikariDataSource dataSource;
  private final Semaphore databaseLock = new Semaphore(1);

  protected DatabaseBackedProcessor(
      ScheduledExecutorService executor, HikariDataSource dataSource) {
    super(executor);
    this.dataSource = dataSource;
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
                    buildDefinitionFromRecord(record),
                    MAPPER.readValue(record.get(WORKFLOW.LABELS).data(), LABELS_JSON_TYPE));
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  protected final ObjectMapper mapper() {
    return MAPPER;
  }

  private final Map<Integer, SoftReference<AtomicBoolean>> liveness = new ConcurrentHashMap<>();

  private AtomicBoolean liveness(int workflowRunId) {
    return liveness
        .computeIfAbsent(workflowRunId, k -> new SoftReference<>(new AtomicBoolean(true)))
        .get();
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
                                      if (record.get(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE)
                                          == Phase.INITIALIZING) {
                                        final var definition = buildDefinitionFromRecord(record);
                                        final var workflow =
                                            DatabaseWorkflow.recover(
                                                record,
                                                liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                dsl);
                                        final var activeOperations =
                                            operations.getOrDefault(
                                                record.get(ACTIVE_WORKFLOW_RUN.ID), List.of());
                                        startRaw.accept(
                                            () ->
                                                recover(
                                                    target,
                                                    definition,
                                                    workflow,
                                                    activeOperations));
                                      } else {
                                        System.err.printf(
                                            "Recovering workflow %s...\n",
                                            record.get(WORKFLOW_RUN.HASH_ID));
                                        recover(
                                            target,
                                            buildDefinitionFromRecord(record),
                                            DatabaseWorkflow.recover(
                                                record,
                                                liveness(record.get(ACTIVE_WORKFLOW_RUN.ID)),
                                                dsl),
                                            operations.getOrDefault(
                                                record.get(ACTIVE_WORKFLOW_RUN.ID), List.of()));
                                      }
                                    }));
              });
      connection.commit();
    }
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
                            r -> r.get(EXTERNAL_ID_VERSION.VALUE)))))
            .entrySet()
            .stream()
            .<FileMetadata>map(
                e ->
                    new FileMetadata() {
                      private final List<ExternalKey> keys =
                          e.getValue().entrySet().stream()
                              .map(
                                  ee ->
                                      new ExternalKey(
                                          ee.getKey().first(), ee.getKey().second(), ee.getValue()))
                              .collect(Collectors.toList());
                      private final String path = e.getKey();

                      @Override
                      public Stream<ExternalKey> externalKeys() {
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
      ObjectNode engineParameters,
      JsonNode metadata,
      Set<ExternalKey> externalKeys,
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
                                                Stream.concat(
                                                        validateInput(
                                                            MAPPER,
                                                            target,
                                                            workflow.definition(),
                                                            arguments,
                                                            metadata,
                                                            engineParameters),
                                                        workflow.validateLabels(labels))
                                                    .collect(Collectors.toSet());
                                            if (!errors.isEmpty()) {
                                              return handler.invalidWorkflow(errors);
                                            }

                                            final var inputIds =
                                                workflow
                                                    .definition()
                                                    .parameters()
                                                    .flatMap(
                                                        p ->
                                                            arguments.has(p.name())
                                                                ? Stream.empty()
                                                                : p.type()
                                                                    .apply(
                                                                        new ExtractInputVidarrIds(
                                                                            MAPPER,
                                                                            arguments.get(
                                                                                p.name()))))
                                                    .collect(Collectors.toCollection(TreeSet::new));

                                            final var unresolvedIds = new TreeSet<String>();

                                            final var externalIds =
                                                workflow
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
                                                                              final var result =
                                                                                  pathForId(id);
                                                                              if (result
                                                                                  .isEmpty()) {
                                                                                unresolvedIds.add(
                                                                                    id);
                                                                              }
                                                                              return result;
                                                                            }))
                                                                : Stream.empty())
                                                    .collect(
                                                        Collectors.toCollection(
                                                            () ->
                                                                new TreeSet<>(
                                                                    Comparator.comparing(
                                                                            ExternalId::getProvider)
                                                                        .thenComparing(
                                                                            ExternalId::getId))));
                                            if (!unresolvedIds.isEmpty()) {
                                              return handler.unresolvedIds(unresolvedIds);
                                            }
                                            if (externalKeys.stream()
                                                .anyMatch(k -> k.getVersions().isEmpty())) {
                                              return handler.missingExternalIdVersion();
                                            }
                                            if (externalIds.size() != externalKeys.size()
                                                || !externalKeys.stream()
                                                    .map(
                                                        k -> new Pair<>(k.getProvider(), k.getId()))
                                                    .collect(Collectors.toSet())
                                                    .equals(
                                                        externalIds.stream()
                                                            .map(
                                                                k ->
                                                                    new Pair<>(
                                                                        k.getProvider(), k.getId()))
                                                            .collect(Collectors.toSet()))) {
                                              return handler.externalIdMismatch();
                                            }
                                            try {
                                              // This is sorted so our output ID is first if we have
                                              // to
                                              // run
                                              final var candidateDigesters =
                                                  transaction
                                                      .select()
                                                      .from(WORKFLOW_VERSION)
                                                      .where(WORKFLOW_VERSION.NAME.eq(name))
                                                      .orderBy(
                                                          DSL.case_()
                                                              .when(
                                                                  WORKFLOW_VERSION.ID.eq(
                                                                      workflow.id()),
                                                                  0)
                                                              .else_(1)
                                                              .asc())
                                                      .fetch(WORKFLOW_VERSION.HASH_ID)
                                                      .stream()
                                                      .map(
                                                          id -> {
                                                            try {
                                                              final var digest =
                                                                  MessageDigest.getInstance(
                                                                      "SHA-256");
                                                              digest.update(
                                                                  id.getBytes(
                                                                      StandardCharsets.UTF_8));
                                                              return digest;
                                                            } catch (NoSuchAlgorithmException e) {
                                                              throw new RuntimeException(e);
                                                            }
                                                          })
                                                      .collect(Collectors.toList());
                                              for (final var id : inputIds) {
                                                final var idBytes =
                                                    hashFromAnalysisId(id)
                                                        .getBytes(StandardCharsets.UTF_8);
                                                for (final var digest : candidateDigesters) {
                                                  digest.update(new byte[] {0});
                                                  digest.update(idBytes);
                                                }
                                              }
                                              for (final var id : externalIds) {
                                                final var buffer = ByteBuffer.allocate(1024);
                                                buffer.put((byte) 0);
                                                buffer.put((byte) 0);
                                                buffer.put(
                                                    id.getProvider()
                                                        .getBytes(StandardCharsets.UTF_8));
                                                buffer.put((byte) 0);
                                                buffer.put(
                                                    id.getId().getBytes(StandardCharsets.UTF_8));
                                                buffer.put((byte) 0);
                                                final var idBytes = buffer.array();
                                                for (final var digest : candidateDigesters) {
                                                  digest.update(idBytes);
                                                }
                                              }
                                              workflow.digestLabels(candidateDigesters, labels);
                                              final var candidateIds =
                                                  candidateDigesters.stream()
                                                      .map(digest -> hexDigits(digest.digest()))
                                                      .collect(Collectors.toList());
                                              final var candidates =
                                                  transaction
                                                      .select(WORKFLOW_RUN.ID, WORKFLOW_RUN.HASH_ID)
                                                      .from(WORKFLOW_RUN)
                                                      .where(WORKFLOW_RUN.HASH_ID.in(candidateIds))
                                                      .stream()
                                                      .map(
                                                          r ->
                                                              new Pair<>(
                                                                  r.get(WORKFLOW_RUN.ID),
                                                                  r.get(WORKFLOW_RUN.HASH_ID)))
                                                      .collect(Collectors.toList());
                                              if (candidates.isEmpty()) {
                                                if (handler.allowLaunch()) {
                                                  final var dbWorkflow =
                                                      DatabaseWorkflow.create(
                                                          targetName,
                                                          workflow.id(),
                                                          candidateIds.get(0),
                                                          labels,
                                                          arguments,
                                                          engineParameters,
                                                          metadata,
                                                          inputIds,
                                                          externalIds,
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
                                                    for (final var entry :
                                                        externalKey.getVersions().entrySet()) {
                                                      externalVersionInsert =
                                                          externalVersionInsert.values(
                                                              DSL.field(
                                                                  DSL.select(EXTERNAL_ID.ID)
                                                                      .from(EXTERNAL_ID)
                                                                      .where(
                                                                          EXTERNAL_ID
                                                                              .EXTERNAL_ID_
                                                                              .eq(
                                                                                  externalKey
                                                                                      .getId())
                                                                              .and(
                                                                                  EXTERNAL_ID
                                                                                      .PROVIDER.eq(
                                                                                      externalKey
                                                                                          .getProvider()))
                                                                              .and(
                                                                                  EXTERNAL_ID
                                                                                      .WORKFLOW_RUN_ID
                                                                                      .eq(
                                                                                          dbWorkflow
                                                                                              .dbId())))),
                                                              DSL.val(entry.getKey()),
                                                              DSL.val(entry.getValue()));
                                                    }
                                                  }
                                                  externalVersionInsert.execute();
                                                  return handler.launched(
                                                      candidateIds.get(0),
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
                                                                      .start(
                                                                          target,
                                                                          workflow.definition(),
                                                                          dbWorkflow,
                                                                          runTransaction));
                                                        }
                                                      });
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
                                                      transaction
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
                                                                                  .eq(
                                                                                      externalKey
                                                                                          .getId())
                                                                                  .and(
                                                                                      EXTERNAL_ID
                                                                                          .PROVIDER
                                                                                          .eq(
                                                                                              externalKey
                                                                                                  .getProvider()))
                                                                                  .and(
                                                                                      EXTERNAL_ID
                                                                                          .WORKFLOW_RUN_ID
                                                                                          .eq(
                                                                                              workflowRunId))))
                                                                  .and(
                                                                      externalKey
                                                                          .getVersions()
                                                                          .entrySet()
                                                                          .stream()
                                                                          .map(
                                                                              e ->
                                                                                  EXTERNAL_ID_VERSION
                                                                                      .KEY
                                                                                      .eq(
                                                                                          e
                                                                                              .getKey())
                                                                                      .and(
                                                                                          EXTERNAL_ID_VERSION
                                                                                              .VALUE
                                                                                              .eq(
                                                                                                  e
                                                                                                      .getValue())))
                                                                          .reduce(Condition::or)
                                                                          .orElseThrow()))
                                                          .fetch(EXTERNAL_ID_VERSION.KEY);

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

                                                var externalVersionInsert =
                                                    transaction
                                                        .insertInto(EXTERNAL_ID_VERSION)
                                                        .columns(
                                                            EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                                                            EXTERNAL_ID_VERSION.KEY,
                                                            EXTERNAL_ID_VERSION.VALUE);
                                                for (final var externalKey : externalKeys) {
                                                  final var matchKeys =
                                                      knownMatches.get(
                                                          new Pair<>(
                                                              externalKey.getProvider(),
                                                              externalKey.getId()));
                                                  transaction
                                                      .update(EXTERNAL_ID_VERSION)
                                                      .set(
                                                          EXTERNAL_ID_VERSION.REQUESTED,
                                                          OffsetDateTime.now())
                                                      .where(
                                                          EXTERNAL_ID_VERSION
                                                              .EXTERNAL_ID_ID
                                                              .eq(
                                                                  DSL.select(EXTERNAL_ID.ID)
                                                                      .from(EXTERNAL_ID)
                                                                      .where(
                                                                          EXTERNAL_ID
                                                                              .EXTERNAL_ID_
                                                                              .eq(
                                                                                  externalKey
                                                                                      .getId())
                                                                              .and(
                                                                                  EXTERNAL_ID
                                                                                      .PROVIDER.eq(
                                                                                      externalKey
                                                                                          .getProvider()))
                                                                              .and(
                                                                                  EXTERNAL_ID
                                                                                      .WORKFLOW_RUN_ID
                                                                                      .eq(
                                                                                          workflowRunId))))
                                                              .and(
                                                                  matchKeys.stream()
                                                                      .map(
                                                                          k ->
                                                                              EXTERNAL_ID_VERSION
                                                                                  .KEY
                                                                                  .eq(k)
                                                                                  .and(
                                                                                      EXTERNAL_ID_VERSION
                                                                                          .VALUE.eq(
                                                                                          externalKey
                                                                                              .getVersions()
                                                                                              .get(
                                                                                                  k))))
                                                                      .reduce(Condition::or)
                                                                      .orElseThrow()))
                                                      .execute();
                                                  for (final var entry :
                                                      externalKey.getVersions().entrySet()) {
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
                                                                            .and(
                                                                                EXTERNAL_ID.PROVIDER
                                                                                    .eq(
                                                                                        externalKey
                                                                                            .getProvider()))
                                                                            .and(
                                                                                EXTERNAL_ID
                                                                                    .WORKFLOW_RUN_ID
                                                                                    .eq(
                                                                                        workflowRunId)))),
                                                            DSL.val(entry.getKey()),
                                                            DSL.val(entry.getValue()));
                                                  }
                                                }
                                                externalVersionInsert.execute();
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
                                                                .and(
                                                                    WORKFLOW_RUN.COMPLETED.isNull())
                                                                .and(
                                                                    ACTIVE_WORKFLOW_RUN
                                                                        .ENGINE_PHASE
                                                                        .eq(Phase.FAILED)
                                                                        .or(
                                                                            DSL.exists(
                                                                                DSL.select()
                                                                                    .from(
                                                                                        ACTIVE_OPERATION)
                                                                                    .where(
                                                                                        ACTIVE_OPERATION
                                                                                            .WORKFLOW_RUN_ID
                                                                                            .eq(
                                                                                                ACTIVE_WORKFLOW_RUN
                                                                                                    .ID)
                                                                                            .and(
                                                                                                ACTIVE_OPERATION
                                                                                                    .ATTEMPT
                                                                                                    .eq(
                                                                                                        ACTIVE_WORKFLOW_RUN
                                                                                                            .ATTEMPT))
                                                                                            .and(
                                                                                                ACTIVE_OPERATION
                                                                                                    .STATUS
                                                                                                    .eq(
                                                                                                        OperationStatus
                                                                                                            .FAILED))))))
                                                                .and(
                                                                    ACTIVE_WORKFLOW_RUN
                                                                        .ATTEMPT
                                                                        .eq(attempt - 1)
                                                                        .or(
                                                                            WORKFLOW_RUN.HASH_ID.ne(
                                                                                candidateIds.get(
                                                                                    0)))))
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
                                                          workflowRunId,
                                                          workflow.id(),
                                                          candidateIds.get(0),
                                                          arguments,
                                                          engineParameters,
                                                          metadata,
                                                          externalIds,
                                                          liveness(workflowRunId),
                                                          externalKeys,
                                                          transaction);
                                                  return handler.reinitialise(
                                                      candidateIds.get(0),
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
                                                                      .start(
                                                                          target,
                                                                          workflow.definition(),
                                                                          dbWorkflow,
                                                                          runTransaction));
                                                        }
                                                      });
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
                } finally {
                  connection.close();
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
}
