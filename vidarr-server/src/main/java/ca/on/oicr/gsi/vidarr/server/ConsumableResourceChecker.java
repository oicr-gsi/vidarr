package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.ACTIVE_WORKFLOW_RUN;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse.Visitor;
import ca.on.oicr.gsi.vidarr.core.Target;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.jooq.Record1;
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

  private static final Counter waitRounds =
      Counter.build(
              "vidarr_consumable_resource_wait_rounds",
              "How many rounds a workflow run had to wait for")
          .labelNames("id")
          .register();

  private static final Counter evaluationExceptions =
      Counter.build(
              "vidarr_consumable_resource_evaluation_exceptions",
              "How many exceptions thrown during resource evaluation")
          .labelNames("resource", "exception")
          .register();
  private final Map<String, JsonNode> consumableResources;
  private final Instant createdTime;
  private final HikariDataSource dataSource;
  private final long dbId;
  private final ScheduledExecutorService executor;
  private final AtomicBoolean isLive;
  private final MaxInFlightByWorkflow maxInFlightByWorkflow;
  private final Runnable next;
  private final Target target;
  private final ObjectNode tracing = Main.MAPPER.createObjectNode();
  private final String vidarrId;
  private final String workflow;
  private final String workflowVersion;

  public ConsumableResourceChecker(
      Target target,
      HikariDataSource dataSource,
      ScheduledExecutorService executor,
      long dbId,
      AtomicBoolean isLive,
      MaxInFlightByWorkflow maxInFlightByWorkflow,
      String workflow,
      String workflowVersion,
      String vidarrId,
      Map<String, JsonNode> consumableResources,
      Instant createdTime,
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
    this.createdTime = createdTime;
    this.next = next;
    this.maxInFlightByWorkflow = maxInFlightByWorkflow;
  }

  @Override
  public void run() {
    if (!isLive.get() || !isWorkflowRunStillPresent()) {
      waitRounds.remove(vidarrId);
      return;
    }
    int i = 0;
    final List<Pair<String, ConsumableResource>> resourceBrokers =
        target.consumableResources().collect(Collectors.toList());
    for (i = 0; i < resourceBrokers.size(); i++) {
      final String resourceName = resourceBrokers.get(i).first();
      final ConsumableResource broker = resourceBrokers.get(i).second();
      Optional<String> error = Optional.empty();
      try {
        error =
            broker
                .request(
                    workflow,
                    workflowVersion,
                    vidarrId,
                    createdTime,
                    maxInFlightByWorkflow.getMaximumFor(workflow),
                    broker.inputFromSubmitter().map(def -> consumableResources.get(def.first())))
                .apply(
                    new Visitor<Optional<String>>() {

                      @Override
                      public Optional<String> available() {
                        return Optional.empty();
                      }

                      @Override
                      public void clear(String name) {
                        tracing.remove(nameForVariable(name));
                      }

                      @Override
                      public Optional<String> error(String message) {
                        return Optional.of(message);
                      }

                      private String nameForVariable(String name) {
                        return String.format("vidarr-resource-%s-%s", resourceName, name);
                      }

                      @Override
                      public void set(String name, long value) {
                        tracing.put(nameForVariable(name), value);
                      }

                      @Override
                      public Optional<String> unavailable() {
                        return Optional.of(
                            String.format("Resource %s is not available", resourceName));
                      }
                    });
      } catch (Exception e) {
        evaluationExceptions.labels(resourceName, e.getClass().getName()).inc();
        e.printStackTrace(System.err);
        error =
            Optional.of(
                String.format(
                    "Evaluating %s threw exception %s", resourceName, e.getClass().getName()));
      }
      if (error.isPresent()) {
        updateBlockedResource(error.get());
        // Each resource performs a check and only releases resources if they've been acquired
        for (final Pair<String, ConsumableResource> b : resourceBrokers.subList(0, i)) {
          b.second()
              .release(
                  workflow,
                  workflowVersion,
                  vidarrId,
                  b.second().inputFromSubmitter().map(def -> consumableResources.get(def.first())));
        }
        executor.schedule(this, 1, TimeUnit.MINUTES);
        return;
      }
    }
    final long waiting = Duration.between(createdTime, Instant.now()).toSeconds();
    tracing.put("vidarr-waiting", waiting);
    updateBlockedResource(null);
    waitTime.labels(workflow).observe(waiting);
    waitRounds.remove(vidarrId);
    next.run();
  }

  private void updateBlockedResource(String error) {
    try (final Connection connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              configuration -> {
                int count =
                    DSL.using(configuration)
                        .update(ACTIVE_WORKFLOW_RUN)
                        .set(ACTIVE_WORKFLOW_RUN.WAITING_RESOURCE, error)
                        .set(ACTIVE_WORKFLOW_RUN.TRACING, tracing)
                        .where(ACTIVE_WORKFLOW_RUN.ID.eq(dbId))
                        .execute();
                if (count == 0) {
                  // the workflow run has been deleted
                  isLive.set(false);
                }
              });
    } catch (SQLException ex) {
      ex.printStackTrace();
    }
    if (isLive.get()) {
      waitRounds.labels(vidarrId).inc();
    } else {
      waitRounds.remove(vidarrId);
    }
  }

  /**
   * Confirm that the workflow run is still tracked in the database (may have been deleted via the
   * API)
   *
   * @return boolean whether the active workflow run is still tracked in the database
   */
  private boolean isWorkflowRunStillPresent() {
    try (final Connection connection = dataSource.getConnection()) {
      return DSL.using(connection, SQLDialect.POSTGRES)
          .transactionResult(
              configuration ->
                  DSL.using(configuration)
                      .select(ACTIVE_WORKFLOW_RUN.ID)
                      .from(ACTIVE_WORKFLOW_RUN)
                      .where(ACTIVE_WORKFLOW_RUN.ID.eq(dbId))
                      .fetchOptional()
                      .isPresent());
    } catch (SQLException ex) {
      ex.printStackTrace();
      return false;
    }
  }
}
