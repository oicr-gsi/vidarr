package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.ACTIVE_WORKFLOW_RUN;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_RUN;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_VERSION;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.SubmissionResultHandler;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.WorkflowInformation;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.Record;

public class ReprovisionStrategyReattempt implements ReprovisionStrategy {

  @Override
  public OffsetDateTime getOriginalCompleted(Record record) {
    JsonNode metadata = record.get(WORKFLOW_RUN.METADATA);
    Iterator<Entry<String, JsonNode>> iterator = metadata.fields();
    while (iterator.hasNext()) {
      Entry<String, JsonNode> entry = iterator.next();
      ArrayNode contents = (ArrayNode) entry.getValue().get("contents");
      Iterator<JsonNode> iterator2 = contents.elements();
      while (iterator2.hasNext()) {
        ObjectNode content = (ObjectNode) iterator2.next();
        if (content.has("originalCompleted")) {
          return OffsetDateTime.ofInstant(
              Instant.ofEpochSecond(content.get("originalCompleted").asInt()),
              ZoneId.of(content.get("originalCompletedOffset").textValue()));
        } // else there's some other kind of content here, maybe the next one
      }
    }
    return OffsetDateTime.MAX;
  }

  @Override
  public JsonNode getMetadata(Record record, String outputPath, String provisionerName,
      OffsetDateTime originalCompleted) {
    return record.get(WORKFLOW_RUN.METADATA);
  }

  @Override
  public DatabaseWorkflow getDbWorkflow(Record record, Target target, JsonNode metadata,
      Map<Integer, Set<ExternalId>> externalIdsByAnalysis, DatabaseBackedProcessor processor,
      DSLContext dsl) {
    record.set(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE, Phase.REPROVISION);
    record.set(ACTIVE_WORKFLOW_RUN.ATTEMPT, record.get(ACTIVE_WORKFLOW_RUN.ATTEMPT) + 1);
    dsl.update(ACTIVE_WORKFLOW_RUN)
        .set(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE, Phase.REPROVISION)
        .set(ACTIVE_WORKFLOW_RUN.ATTEMPT, record.get(ACTIVE_WORKFLOW_RUN.ATTEMPT))
        .execute();
    return DatabaseWorkflow.recover(target, record, processor.liveness(record.get(WORKFLOW_RUN.ID)),
        dsl);
  }

  @Override
  public <T> T handle(Record record, DatabaseWorkflow dbWorkflow,
      WorkflowInformation definition,
      Map<ProvenanceAnalysisRecord<ExternalId>, JsonNode> analysis,
      OffsetDateTime originalCompleted,
      SubmissionResultHandler<T> handler, Target target,
      HikariDataSource dataSource, ScheduledExecutorService executor,
      DatabaseBackedProcessor processor) {
    return handler.reinitialise(record.get(WORKFLOW_RUN.HASH_ID),
        new ConsumableResourceChecker(
            target,
            dataSource,
            executor,
            dbWorkflow.dbId(),
            processor.liveness(dbWorkflow.dbId()),
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
                processor.inTransaction(
                    runTransaction ->
                        processor.reprovision(
                            target, definition.definition(),
                            dbWorkflow, analysis, originalCompleted,
                            runTransaction));
              }
            }));
  }
}
