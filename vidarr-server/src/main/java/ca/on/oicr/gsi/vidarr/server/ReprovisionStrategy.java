package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.SubmissionResultHandler;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.WorkflowInformation;
import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.jooq.DSLContext;
import org.jooq.Record;

public interface ReprovisionStrategy {

  OffsetDateTime getOriginalCompleted(Record record);

  JsonNode getMetadata(Record record, String outputPath,
      String provisionerName, OffsetDateTime originalCompleted);

  DatabaseWorkflow getDbWorkflow(Record record,
      Target target,
      JsonNode metadata,
      Map<Integer, Set<ExternalId>> externalIdsByAnalysis,
      DatabaseBackedProcessor processor,
      DSLContext dsl) throws SQLException;

  <T> T handle(Record record,
      DatabaseWorkflow dbWorkflow,
      WorkflowInformation definition,
      Map<ProvenanceAnalysisRecord<ExternalId>, JsonNode> analysis,
      OffsetDateTime originalCompleted,
      SubmissionResultHandler<T> handler,
      Target target,
      HikariDataSource dataSource,
      ScheduledExecutorService executor,
      DatabaseBackedProcessor processor);
}
