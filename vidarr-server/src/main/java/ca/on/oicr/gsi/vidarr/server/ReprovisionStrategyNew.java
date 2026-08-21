package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.WORKFLOW_RUN;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.SubmissionResultHandler;
import com.zaxxer.hikari.HikariDataSource;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.jooq.DSLContext;
import org.jooq.Record;
import tools.jackson.databind.JsonNode;

public class ReprovisionStrategyNew implements ReprovisionStrategy {

  @Override
  public OffsetDateTime getOriginalCompleted(Record record, Optional<OffsetDateTime> originalCompleted)
    throws DateTimeException {
    OffsetDateTime dateTime = originalCompleted.orElseGet(() -> record.get(WORKFLOW_RUN.COMPLETED));
    if (null == dateTime){
      throw new DateTimeException(String.format(
          "Workflow run %s doesn't seem to have a valid original completed time.",
          record.get(WORKFLOW_RUN.ID)));
    }
    return dateTime;
  }

  @Override
  public DatabaseWorkflow getDbWorkflow(
      Record record,
      Target target,
      JsonNode metadata,
      Set<ExternalId> externalIds,
      DatabaseBackedProcessor processor,
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
        externalIds,
        Map.of(), // empty consumable resources
        record.get(WORKFLOW_RUN.CREATED).toInstant(),
        processor::liveness,
        dsl,
        Phase.REPROVISION);
  }

  @Override
  public <T> T handle(
      Record record,
      DatabaseWorkflow dbWorkflow,
      DatabaseBackedProcessor.WorkflowInformation definition,
      Map<ProvenanceAnalysisRecord<ExternalId>, JsonNode> analysis,
      OffsetDateTime originalCompleted,
      SubmissionResultHandler<T> handler,
      Target target,
      HikariDataSource dataSource,
      ScheduledExecutorService executor,
      DatabaseBackedProcessor processor) {
    return handler.launched(
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
