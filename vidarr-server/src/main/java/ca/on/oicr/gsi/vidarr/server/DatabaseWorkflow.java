package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.*;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.core.ActiveWorkflow;
import ca.on.oicr.gsi.vidarr.core.BaseProcessor;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.Target;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;

public class DatabaseWorkflow implements ActiveWorkflow<DatabaseOperation, DSLContext> {
  public static DatabaseWorkflow create(
      String targetName,
      Target target,
      int workflowVersionId,
      String workflowName,
      String workflowVersion,
      String vidarrId,
      ObjectNode labels,
      JsonNode arguments,
      JsonNode engineParameters,
      JsonNode metadata,
      SortedSet<String> fileIds,
      Set<? extends ExternalId> ids,
      Map<String, JsonNode> consumableResources,
      IntFunction<AtomicBoolean> liveness,
      DSLContext dsl)
      throws SQLException {
    final int dbId =
        dsl.insertInto(WORKFLOW_RUN)
            .columns(
                WORKFLOW_RUN.ARGUMENTS,
                WORKFLOW_RUN.METADATA,
                WORKFLOW_RUN.ENGINE_PARAMETERS,
                WORKFLOW_RUN.WORKFLOW_VERSION_ID,
                WORKFLOW_RUN.HASH_ID,
                WORKFLOW_RUN.LABELS,
                WORKFLOW_RUN.INPUT_FILE_IDS)
            .values(
                arguments,
                metadata,
                engineParameters,
                workflowVersionId,
                vidarrId,
                labelsToJson(labels),
                fileIds.toArray(String[]::new))
            .returningResult(WORKFLOW_RUN.ID)
            .fetchOptional()
            .orElseThrow()
            .value1();

    var idQuery =
        dsl.insertInto(EXTERNAL_ID)
            .columns(EXTERNAL_ID.WORKFLOW_RUN_ID, EXTERNAL_ID.PROVIDER, EXTERNAL_ID.EXTERNAL_ID_);
    for (final var id : ids) {
      idQuery = idQuery.values(dbId, id.getProvider(), id.getId());
    }
    idQuery.execute();

    dsl.insertInto(ACTIVE_WORKFLOW_RUN)
        .columns(
            ACTIVE_WORKFLOW_RUN.ID,
            ACTIVE_WORKFLOW_RUN.ENGINE_PHASE,
            ACTIVE_WORKFLOW_RUN.EXTERNAL_INPUT_IDS_HANDLED,
            ACTIVE_WORKFLOW_RUN.PREFLIGHT_OKAY,
            ACTIVE_WORKFLOW_RUN.TARGET,
            ACTIVE_WORKFLOW_RUN.CONSUMABLE_RESOURCES)
        .values(
            dbId,
            Phase.WAITING_FOR_RESOURCES,
            false,
            true,
            targetName,
            Main.MAPPER.valueToTree(consumableResources))
        .execute();

    return new DatabaseWorkflow(
        target,
        dbId,
        vidarrId,
        0,
        workflowName,
        workflowVersion,
        arguments,
        engineParameters,
        metadata,
        null,
        false,
        new HashSet<>(ids),
        Collections.emptySet(),
        true,
        Phase.WAITING_FOR_RESOURCES,
        null,
        liveness.apply(dbId));
  }

  private static JSONB labelsToJson(Map<String, String> labels) {
    final var labelNode = JsonNodeFactory.instance.objectNode();
    for (final var label : labels.entrySet()) {
      labelNode.put(label.getKey(), label.getValue());
    }
    try {
      return JSONB.valueOf(DatabaseBackedProcessor.MAPPER.writeValueAsString(labelNode));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static JSONB labelsToJson(ObjectNode labels) {
    try {
      return JSONB.valueOf(DatabaseBackedProcessor.MAPPER.writeValueAsString(labels));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static DatabaseWorkflow recover(
      Target target, Record record, AtomicBoolean liveness, DSLContext dsl) {
    final var inputIds = new HashSet<ExternalId>();
    final var requestedInputIds = new HashSet<ExternalId>();
    dsl.select(EXTERNAL_ID.asterisk())
        .from(EXTERNAL_ID)
        .where(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(record.get(ACTIVE_WORKFLOW_RUN.ID)))
        .forEach(
            externalIdRecord -> {
              final var externalId =
                  new ExternalId(
                      externalIdRecord.get(EXTERNAL_ID.PROVIDER),
                      externalIdRecord.get(EXTERNAL_ID.EXTERNAL_ID_));
              inputIds.add(externalId);
              if (externalIdRecord.get(EXTERNAL_ID.REQUESTED)) {
                requestedInputIds.add(externalId);
              }
            });

    return new DatabaseWorkflow(
        target,
        record.get(ACTIVE_WORKFLOW_RUN.ID),
        record.get(WORKFLOW_RUN.HASH_ID),
        record.get(ACTIVE_WORKFLOW_RUN.ATTEMPT),
        record.get(WORKFLOW_VERSION.NAME),
        record.get(WORKFLOW_VERSION.VERSION),
        record.get(WORKFLOW_RUN.ARGUMENTS),
        record.get(WORKFLOW_RUN.ENGINE_PARAMETERS),
        record.get(WORKFLOW_RUN.METADATA),
        record.get(ACTIVE_WORKFLOW_RUN.CLEANUP_STATE),
        record.get(ACTIVE_WORKFLOW_RUN.EXTERNAL_INPUT_IDS_HANDLED),
        inputIds,
        requestedInputIds,
        record.get(ACTIVE_WORKFLOW_RUN.PREFLIGHT_OKAY),
        record.get(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE),
        record.get(ACTIVE_WORKFLOW_RUN.REAL_INPUT).isNull()
            ? null
            : (ObjectNode) record.get(ACTIVE_WORKFLOW_RUN.REAL_INPUT),
        liveness);
  }

  public static DatabaseWorkflow reinitialise(
      Target target,
      int dbId,
      int workflowVersionId,
      String workflowName,
      String workflowVersion,
      String vidarrId,
      JsonNode arguments,
      JsonNode engineParameters,
      JsonNode metadata,
      Set<? extends ExternalId> ids,
      AtomicBoolean liveness,
      Set<ExternalKey> keys,
      Map<String, JsonNode> consumableResources,
      DSLContext dsl)
      throws SQLException {
    final var attempt =
        dsl.update(ACTIVE_WORKFLOW_RUN)
            .set(ACTIVE_WORKFLOW_RUN.ATTEMPT, ACTIVE_WORKFLOW_RUN.ATTEMPT.plus(1))
            .set(
                ACTIVE_WORKFLOW_RUN.CONSUMABLE_RESOURCES,
                Main.MAPPER.<JsonNode>valueToTree(consumableResources))
            .set(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE, Phase.WAITING_FOR_RESOURCES)
            .set(ACTIVE_WORKFLOW_RUN.EXTERNAL_INPUT_IDS_HANDLED, false)
            .set(ACTIVE_WORKFLOW_RUN.PREFLIGHT_OKAY, true)
            .where(ACTIVE_WORKFLOW_RUN.ID.eq(dbId))
            .returningResult(ACTIVE_WORKFLOW_RUN.ATTEMPT)
            .fetchOne()
            .value1();
    dsl.update(WORKFLOW_RUN)
        .set(WORKFLOW_RUN.ARGUMENTS, arguments)
        .set(WORKFLOW_RUN.ENGINE_PARAMETERS, engineParameters)
        .set(WORKFLOW_RUN.HASH_ID, vidarrId)
        .set(WORKFLOW_RUN.METADATA, metadata)
        .set(WORKFLOW_RUN.WORKFLOW_VERSION_ID, workflowVersionId)
        .where(WORKFLOW_RUN.ID.eq(dbId))
        .execute();

    dsl.delete(ANALYSIS_EXTERNAL_ID)
        .where(
            ANALYSIS_EXTERNAL_ID.ANALYSIS_ID.in(
                DSL.select(ANALYSIS.ID).from(ANALYSIS).where(ANALYSIS.WORKFLOW_RUN_ID.eq(dbId))))
        .execute();
    dsl.delete(ANALYSIS).where(ANALYSIS.WORKFLOW_RUN_ID.eq(dbId)).execute();

    // Since the workflow previously failed, we're just going to recreate all the versions since
    // equivalency relationships are sus.
    dsl.delete(EXTERNAL_ID_VERSION)
        .where(
            EXTERNAL_ID_VERSION.EXTERNAL_ID_ID.in(
                DSL.select(EXTERNAL_ID.ID)
                    .from(EXTERNAL_ID)
                    .where(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(dbId))));
    var externalVersionsInsert =
        dsl.insertInto(EXTERNAL_ID_VERSION)
            .columns(
                EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                EXTERNAL_ID_VERSION.KEY,
                EXTERNAL_ID_VERSION.VALUE);
    for (final var externalKey : keys) {
      for (final var version : externalKey.getVersions().entrySet()) {
        externalVersionsInsert =
            externalVersionsInsert.values(
                DSL.field(
                    DSL.select(EXTERNAL_ID.ID)
                        .from(EXTERNAL_ID)
                        .where(
                            EXTERNAL_ID
                                .PROVIDER
                                .eq(externalKey.getProvider())
                                .and(EXTERNAL_ID.EXTERNAL_ID_.eq(externalKey.getId()))
                                .and(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(dbId)))),
                DSL.val(version.getKey()),
                DSL.val(version.getValue()));
      }
    }

    return new DatabaseWorkflow(
        target,
        dbId,
        vidarrId,
        attempt,
        workflowName,
        workflowVersion,
        arguments,
        engineParameters,
        metadata,
        null,
        false,
        new HashSet<>(ids),
        Collections.emptySet(),
        true,
        Phase.WAITING_FOR_RESOURCES,
        null,
        liveness);
  }

  private final JsonNode arguments;
  private final int attempt;
  private JsonNode cleanup;
  private final JsonNode engineArguments;
  private boolean extraInputIdsHandled;
  private final int id;
  private final Set<ExternalId> inputIds;
  private boolean isPreflightOkay;
  private final AtomicBoolean liveness;
  private final JsonNode metadata;
  private Phase phase;
  private ObjectNode realInput;
  private Set<ExternalId> requestedInputIds;
  private final Target target;
  private final String vidarrId;
  private final String workflowName;
  private final String workflowVersion;

  private DatabaseWorkflow(
      Target target,
      int id,
      String vidarrId,
      int attempt,
      String workflowName,
      String workflowVersion,
      JsonNode arguments,
      JsonNode engineArguments,
      JsonNode metadata,
      JsonNode cleanup,
      boolean extraInputIdsHandled,
      Set<ExternalId> inputIds,
      Set<ExternalId> requestedInputIds,
      boolean isPreflightOkay,
      Phase phase,
      ObjectNode realInput,
      AtomicBoolean liveness) {
    this.target = target;
    this.attempt = attempt;
    this.workflowName = workflowName;
    this.workflowVersion = workflowVersion;
    this.arguments = arguments;
    this.engineArguments = engineArguments;
    this.id = id;
    this.vidarrId = vidarrId;
    this.metadata = metadata;
    this.cleanup = cleanup;
    this.extraInputIdsHandled = extraInputIdsHandled;
    this.inputIds = inputIds;
    this.requestedInputIds = requestedInputIds;
    this.isPreflightOkay = isPreflightOkay;
    this.phase = phase;
    this.realInput = realInput;
    this.liveness = liveness;
  }

  @Override
  public JsonNode arguments() {
    return arguments;
  }

  public void attachExternalIds(
      org.jooq.DSLContext dsl, int recordId, Set<? extends ExternalId> ids) {
    var insertStatement =
        dsl.insertInto(ANALYSIS_EXTERNAL_ID)
            .columns(ANALYSIS_EXTERNAL_ID.ANALYSIS_ID, ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID);

    for (final var id : ids) {
      insertStatement =
          insertStatement.values(
              DSL.val(recordId),
              DSL.field(
                  DSL.select(EXTERNAL_ID.ID)
                      .from(EXTERNAL_ID)
                      .where(
                          EXTERNAL_ID
                              .EXTERNAL_ID_
                              .eq(id.getId())
                              .and(EXTERNAL_ID.PROVIDER.eq(id.getProvider()))
                              .and(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(this.id)))));
    }
    insertStatement.execute();
  }

  @Override
  public JsonNode cleanup() {
    return cleanup;
  }

  @Override
  public void cleanup(JsonNode cleanupState, DSLContext transaction) {
    this.cleanup = cleanupState;
    updateField(ACTIVE_WORKFLOW_RUN.CLEANUP_STATE, cleanupState, transaction);
  }

  public int dbId() {
    return id;
  }

  @Override
  public JsonNode engineArguments() {
    return engineArguments;
  }

  @Override
  public boolean extraInputIdsHandled() {
    return extraInputIdsHandled;
  }

  @Override
  public void extraInputIdsHandled(boolean extraInputIdsHandled, DSLContext transaction) {
    this.extraInputIdsHandled = extraInputIdsHandled;
    updateField(ACTIVE_WORKFLOW_RUN.EXTERNAL_INPUT_IDS_HANDLED, extraInputIdsHandled, transaction);
  }

  @Override
  public String id() {
    return vidarrId;
  }

  @Override
  public Set<ExternalId> inputIds() {
    return new HashSet<>(inputIds);
  }

  @Override
  public boolean isPreflightOkay() {
    return isPreflightOkay;
  }

  @Override
  public JsonNode metadata() {
    return metadata;
  }

  @Override
  public Phase phase() {
    return phase;
  }

  @Override
  public List<DatabaseOperation> phase(
      Phase phase, List<Pair<String, JsonNode>> operationInitialStates, DSLContext transaction) {
    if (liveness.get()) {
      if (this.phase == Phase.INITIALIZING || this.phase == Phase.WAITING_FOR_RESOURCES) {
        updateMainField(WORKFLOW_RUN.STARTED, OffsetDateTime.now(), transaction);
      }
      this.phase = phase;
      updateField(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE, phase, transaction);
      if (phase == Phase.FAILED) {
        target
            .consumableResources()
            .forEach(cr -> cr.release(workflowName, workflowVersion, vidarrId));
      }
      return operationInitialStates.stream()
          .map(
              state ->
                  DatabaseOperation.create(
                          transaction,
                          id,
                          phase,
                          state.first(),
                          state.second(),
                          attempt,
                          liveness,
                          this)
                      .orElseThrow())
          .collect(Collectors.toList());
    } else {
      // The world has passed us by, bail out.
      return Collections.emptyList();
    }
  }

  @Override
  public void preflightFailed(DSLContext transaction) {
    isPreflightOkay = false;
    updateField(ACTIVE_WORKFLOW_RUN.PREFLIGHT_OKAY, false, transaction);
  }

  @Override
  public void provisionFile(
      Set<? extends ExternalId> ids,
      String storagePath,
      String md5,
      String metatype,
      long fileSize,
      Map<String, String> labels,
      DSLContext dsl) {
    if (liveness.get()) {
      try {
        final var digest = MessageDigest.getInstance("SHA-256");
        digest.update(vidarrId.getBytes(StandardCharsets.UTF_8));
        digest.update(
            Path.of(storagePath).getFileName().toString().getBytes(StandardCharsets.UTF_8));

        final var recordId =
            dsl.insertInto(ANALYSIS)
                .set(ANALYSIS.ANALYSIS_TYPE, "file")
                .set(ANALYSIS.FILE_MD5SUM, md5)
                .set(ANALYSIS.FILE_METATYPE, metatype)
                .set(ANALYSIS.FILE_PATH, storagePath)
                .set(ANALYSIS.FILE_SIZE, fileSize)
                .set(ANALYSIS.HASH_ID, BaseProcessor.hexDigits(digest.digest()))
                .set(ANALYSIS.LABELS, labelsToJson(labels))
                .set(ANALYSIS.WORKFLOW_RUN_ID, id)
                .returningResult(ANALYSIS.ID)
                .fetchOptional()
                .orElseThrow()
                .value1();
        attachExternalIds(dsl, recordId, ids);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void provisionUrl(
      Set<? extends ExternalId> ids, String url, Map<String, String> labels, DSLContext dsl) {
    if (liveness.get()) {
      try {
        final var digest = MessageDigest.getInstance("SHA-256");
        digest.update(vidarrId.getBytes(StandardCharsets.UTF_8));
        digest.update(url.getBytes(StandardCharsets.UTF_8));
        final var recordId =
            dsl.insertInto(ANALYSIS)
                .set(ANALYSIS.ANALYSIS_TYPE, "url")
                .set(ANALYSIS.FILE_PATH, url)
                .set(ANALYSIS.HASH_ID, BaseProcessor.hexDigits(digest.digest()))
                .set(ANALYSIS.LABELS, labelsToJson(labels))
                .set(ANALYSIS.WORKFLOW_RUN_ID, id)
                .returningResult(ANALYSIS.ID)
                .fetchOptional()
                .orElseThrow()
                .value1();
        attachExternalIds(dsl, recordId, ids);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public ObjectNode realInput() {
    return realInput;
  }

  @Override
  public void realInput(ObjectNode realInput, DSLContext transaction) {
    this.realInput = realInput;
    updateField(ACTIVE_WORKFLOW_RUN.REAL_INPUT, realInput, transaction);
  }

  @Override
  public Set<ExternalId> requestedExternalIds() {
    return new HashSet<>(requestedInputIds);
  }

  @Override
  public void requestedExternalIds(Set<ExternalId> requiredExternalIds, DSLContext dsl) {
    this.requestedInputIds = requiredExternalIds;
    requiredExternalIds.stream()
        .map(
            id ->
                EXTERNAL_ID
                    .PROVIDER
                    .eq(id.getProvider())
                    .and(EXTERNAL_ID.EXTERNAL_ID_.eq(id.getId())))
        .reduce(Condition::or)
        .ifPresent(
            condition ->
                dsl.update(EXTERNAL_ID)
                    .set(EXTERNAL_ID.REQUESTED, true)
                    .where(condition)
                    .execute());
  }

  @Override
  public void runUrl(String workflowRunUrl, DSLContext transaction) {
    updateField(ACTIVE_WORKFLOW_RUN.WORKFLOW_RUN_URL, workflowRunUrl, transaction);
  }

  @Override
  public void succeeded(DSLContext transaction) {
    updateMainField(WORKFLOW_RUN.COMPLETED, OffsetDateTime.now(), transaction);
    transaction
        .deleteFrom(ACTIVE_OPERATION)
        .where(ACTIVE_OPERATION.WORKFLOW_RUN_ID.eq(id))
        .execute();
    transaction.deleteFrom(ACTIVE_WORKFLOW_RUN).where(ACTIVE_WORKFLOW_RUN.ID.eq(id)).execute();
    target.consumableResources().forEach(cr -> cr.release(workflowName, workflowVersion, vidarrId));
  }

  private <T> void updateField(Field<T> field, T value, DSLContext dsl) {
    dsl.update(ACTIVE_WORKFLOW_RUN)
        .set(field, value)
        .where(ACTIVE_WORKFLOW_RUN.ID.eq(id))
        .execute();
  }

  private <T> void updateMainField(Field<T> field, T value, DSLContext dsl) {
    dsl.update(WORKFLOW_RUN).set(field, value).where(WORKFLOW_RUN.ID.eq(id)).execute();
  }
}
