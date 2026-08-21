package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.ACTIVE_WORKFLOW_RUN;
import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_RUN;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.SubmissionResultHandler;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.WorkflowInformation;
import com.zaxxer.hikari.HikariDataSource;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.jooq.DSLContext;
import org.jooq.Record;
import tools.jackson.databind.JsonNode;

public class ReprovisionStrategyReattempt implements ReprovisionStrategy {

  @Override
  public OffsetDateTime getOriginalCompleted(Record record, Optional<OffsetDateTime> originalCompleted)
  throws DateTimeException {
    JsonNode metadata = DatabaseBackedProcessor.getLatestProvision(record.get(WORKFLOW_RUN.METADATA));

    // TODO there's gotta be a better way of doing this, but i don't know it.
    // Safe only because the values should be the same across all reprovisions.
    Optional<Instant> jsonOriginalCompleted = DatabaseBackedProcessor.streamJackson(metadata)
        .filter(e -> e.getKey().equals("originalCompleted"))
        .findFirst()
        .map(e -> Instant.ofEpochSecond(e.getValue().asInt()));

    Optional<ZoneId> jsonOriginalZone = DatabaseBackedProcessor.streamJackson(metadata)
        .filter(e -> e.getKey().equals("originalCompletedOffset"))
        .findFirst()
        .map(e -> ZoneId.of(e.getValue().stringValue()));

    if(jsonOriginalCompleted.isPresent() && jsonOriginalZone.isPresent()) {
      return OffsetDateTime.ofInstant(jsonOriginalCompleted.get(), jsonOriginalZone.get());
    } else {
      throw new DateTimeException(String.format(
          "Workflow run %s doesn't seem to have a valid original completed time.",
          record.get(WORKFLOW_RUN.ID)));
    }
  }

  @Override
  public DatabaseWorkflow getDbWorkflow(
      Record record,
      Target target,
      JsonNode metadata,
      Set<ExternalId> externalIds,
      DatabaseBackedProcessor processor,
      DSLContext dsl) {
    record.set(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE, Phase.REPROVISION);
    record.set(ACTIVE_WORKFLOW_RUN.ATTEMPT, record.get(ACTIVE_WORKFLOW_RUN.ATTEMPT) + 1);
    dsl.update(ACTIVE_WORKFLOW_RUN)
        .set(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE, Phase.REPROVISION)
        .set(ACTIVE_WORKFLOW_RUN.ATTEMPT, record.get(ACTIVE_WORKFLOW_RUN.ATTEMPT))
        .where(ACTIVE_WORKFLOW_RUN.ID.eq(record.get(ACTIVE_WORKFLOW_RUN.ID)))
        .execute();
    return DatabaseWorkflow.recover(
        target, record, processor.liveness(record.get(WORKFLOW_RUN.ID)), dsl);
  }

  @Override
  public <T> T handle(
      Record record,
      DatabaseWorkflow dbWorkflow,
      WorkflowInformation definition,
      Map<ProvenanceAnalysisRecord<ExternalId>, JsonNode> analysis,
      OffsetDateTime originalCompleted,
      SubmissionResultHandler<T> handler,
      Target target,
      HikariDataSource dataSource,
      ScheduledExecutorService executor,
      DatabaseBackedProcessor processor) {
    return handler.reinitialise(
        record.get(WORKFLOW_RUN.HASH_ID),
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
                  throw new IllegalStateException("Workflow has already been" + " launched");
                }
                launched = true;
                processor.inTransaction(
                    runTransaction ->
                        processor.reprovision(
                            target,
                            definition.definition(),
                            dbWorkflow,
                            analysis,
                            originalCompleted,
                            runTransaction));
              }
            }));
  }
}
