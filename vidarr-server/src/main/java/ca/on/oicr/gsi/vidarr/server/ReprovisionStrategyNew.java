package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_RUN;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.SubmissionResultHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.jooq.DSLContext;
import org.jooq.Record;

public class ReprovisionStrategyNew implements ReprovisionStrategy {

  @Override
  public OffsetDateTime getOriginalCompleted(Record record) {
    return record.get(WORKFLOW_RUN.COMPLETED);
  }

  @Override
  public JsonNode getMetadata(Record record, String outputPath, String provisionerName,
      OffsetDateTime originalCompleted) {
    JsonNode metadata = record.get(WORKFLOW_RUN.METADATA);
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
          content.put("originalCompleted",
              originalCompleted.toInstant().getEpochSecond());
          content.put("originalCompletedOffset",
              originalCompleted.getOffset().toString());
        } // else there's some other kind of content here, maybe the next one
      }
    }
    return metadata;
  }

  @Override
  public DatabaseWorkflow getDbWorkflow(Record record, Target target, JsonNode metadata,
      Map<Integer, Set<ExternalId>> externalIdsByAnalysis, DatabaseBackedProcessor processor,
      DSLContext dsl) {
    return DatabaseWorkflow.createActive(
        "reprovision",
        target,
        record.get(WORKFLOW_RUN.ID),
        "reprovision",
        "1",
        record.get(WORKFLOW_RUN.HASH_ID),
        record.get(WORKFLOW_RUN.ARGUMENTS),
        record.get(WORKFLOW_RUN.ENGINE_PARAMETERS),
        metadata,
        externalIdsByAnalysis.values().stream().flatMap(Collection::stream).collect(
            Collectors.toSet()),
        Map.of(), //empty consumable resources
        record.get(WORKFLOW_RUN.CREATED).toInstant(),
        processor::liveness,
        dsl,
        Phase.REPROVISION
    );
  }

  @Override
  public <T> T handle(Record record,
      DatabaseWorkflow dbWorkflow,
      DatabaseBackedProcessor.WorkflowInformation definition,
      Map<ProvenanceAnalysisRecord<ExternalId>, JsonNode> analysis,
      OffsetDateTime originalCompleted,
      SubmissionResultHandler<T> handler, Target target,
      HikariDataSource dataSource, ScheduledExecutorService executor,
      DatabaseBackedProcessor processor) {
    return handler.launched(record.get(WORKFLOW_RUN.HASH_ID),
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
