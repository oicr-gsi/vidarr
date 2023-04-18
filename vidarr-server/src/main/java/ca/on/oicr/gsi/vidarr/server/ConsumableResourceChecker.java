package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.ACTIVE_WORKFLOW_RUN;

import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse.Visitor;
import ca.on.oicr.gsi.vidarr.core.Target;
import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariDataSource;
import io.prometheus.client.Histogram;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

final class ConsumableResourceChecker implements Runnable {
  private static final Histogram waitTime =
      Histogram.build(
              "vidarr_consumable_resource_wait_time",
              "A histogram of the waiting time, in seconds, of how long a workflow run waited for"
                  + " resources.")
          .buckets(60, 300, 600, 3600, 7200, 21600, 43200, 86400, 129600, 172800)
          .labelNames("workflow")
          .register();

  private final Map<String, JsonNode> consumableResources;
  private final HikariDataSource dataSource;
  private final long dbId;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean isLive;
  private final Runnable next;
  private final Instant startTime = Instant.now();
  private final Target target;
  private final String vidarrId;
  private final String workflow;
  private final String workflowVersion;

  public ConsumableResourceChecker(
      Target target,
      HikariDataSource dataSource,
      ScheduledExecutorService executor,
      long dbId,
      AtomicBoolean isLive,
      String workflow,
      String workflowVersion,
      String vidarrId,
      Map<String, JsonNode> consumableResources,
      Runnable next) {
    this.target = target;
    this.dataSource = dataSource;
    this.executor = executor;
    this.dbId = dbId;
    this.isLive = isLive;
    this.workflow = workflow;
    this.workflowVersion = workflowVersion;
    this.vidarrId = vidarrId;
    this.consumableResources = consumableResources;
    this.next = next;
  }

  @Override
  public void run() {
    if (!isLive.get()) {
      return;
    }
    var i = 0;
    final var resourceBrokers = target.consumableResources().collect(Collectors.toList());
    for (i = 0; i < resourceBrokers.size(); i++) {
      final var name = resourceBrokers.get(i).first();
      final var broker = resourceBrokers.get(i).second();
      final var error =
          broker
              .request(
                  workflow,
                  workflowVersion,
                  vidarrId,
                  broker.inputFromSubmitter().map(def -> consumableResources.get(def.first())))
              .apply(
                  new Visitor<Optional<String>>() {
                    @Override
                    public Optional<String> available() {
                      return Optional.empty();
                    }

                    @Override
                    public Optional<String> error(String message) {
                      return Optional.of(message);
                    }

                    @Override
                    public Optional<String> unavailable() {
                      return Optional.of(String.format("Resource %s is not available", name));
                    }
                  });
      if (error.isPresent()) {
        updateBlockedResource(error.get());
        // Skip the last one, because it already failed, so we don't have to release any resources.
        while (--i >= 0) {
          resourceBrokers.get(i).second().release(workflow, workflowVersion, vidarrId);
        }
        executor.schedule(this, 2, TimeUnit.MINUTES);
        return;
      }
    }
    updateBlockedResource(null);
    waitTime.labels(workflow).observe(Duration.between(startTime, Instant.now()).toSeconds());
    next.run();
  }

  private void updateBlockedResource(String error) {
    try (final var connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              configuration ->
                  DSL.using(configuration)
                      .update(ACTIVE_WORKFLOW_RUN)
                      .set(ACTIVE_WORKFLOW_RUN.WAITING_RESOURCE, error)
                      .where(ACTIVE_WORKFLOW_RUN.ID.eq(dbId))
                      .execute());

    } catch (SQLException ex) {
      ex.printStackTrace();
    }
  }
}
