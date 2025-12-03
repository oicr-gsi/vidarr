package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.core.BaseProcessor.hexDigits;
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
import static org.jooq.impl.DSL.param;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import ca.on.oicr.gsi.status.ConfigurationSection;
import ca.on.oicr.gsi.status.Header;
import ca.on.oicr.gsi.status.NavigationMenu;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.status.ServerConfig;
import ca.on.oicr.gsi.status.StatusPage;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import ca.on.oicr.gsi.vidarr.JsonPost;
import ca.on.oicr.gsi.vidarr.OperationStatus;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import ca.on.oicr.gsi.vidarr.api.*;
import ca.on.oicr.gsi.vidarr.core.BaseProcessor;
import ca.on.oicr.gsi.vidarr.core.ExtractInputVidarrIds;
import ca.on.oicr.gsi.vidarr.core.FileMetadata;
import ca.on.oicr.gsi.vidarr.core.ManualOverrideConsumableResource;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.DeleteResultHandler;
import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.ExternalIdVersion;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.GetIdsForDownstreamWorkflowRuns;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.WorkflowVersion;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.records.AnalysisExternalIdRecord;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.records.ExternalIdVersionRecord;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.records.WorkflowVersionAccessoryRecord;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.server.handlers.ExceptionHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.util.Headers;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStep2;
import org.jooq.InsertValuesStep3;
import org.jooq.JSON;
import org.jooq.JSONB;
import org.jooq.JSONEntry;
import org.jooq.JSONObjectNullStep;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.postgresql.ds.PGSimpleDataSource;

public final class Main implements ServerConfig {

  private interface UnloadProcessor<T> {
    T process(Configuration configuration, Long[] workflowRuns) throws IOException, SQLException;
  }

  static final HttpClient CLIENT =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .followRedirects(HttpClient.Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(20))
          .build();
  // Jdk8Module is a compatibility fix for de/serializing Optionals
  static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
  static final JsonFactory MAPPER_FACTORY = new JsonFactory().setCodec(MAPPER);
  private static final String CONTENT_TYPE_TEXT = "text/plain";
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final Counter REMOTE_ERROR_COUNT =
      Counter.build(
              "vidarr_remote_vidarr_error_count",
              "The number of times a remote instance returned an error.")
          .labelNames("remote")
          .register();
  private static final Counter PROVENANCE_ERROR_COUNT =
      Counter.build(
              "vidarr_provenance_error_count",
              "The number of times Vidarr encountered an error when building a provenance "
                  + "response")
          .register();
  private static final LatencyHistogram REMOTE_RESPONSE_TIME =
      new LatencyHistogram(
          "vidarr_remote_vidarr_response_time",
          "The response time of a remote instance to a metadata access request",
          "remote");
  private static final LatencyHistogram RESPONSE_TIME =
      new LatencyHistogram(
          "vidarr_http_response_time", "The response time to serve a query", "url");
  private static final List<JSONEntry<?>> STATUS_FIELDS = new ArrayList<>();

  static {
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    STATUS_FIELDS.add(literalJsonEntry("completed", WORKFLOW_RUN.COMPLETED));
    STATUS_FIELDS.add(
        literalJsonEntry(
            "operationStatus",
            DSL.case_(
                    DSL.nvl(
                        DSL.field(
                            DSL.select(
                                    DSL.max(
                                        DSL.case_(ACTIVE_OPERATION.STATUS)
                                            .when(DSL.inline(OperationStatus.FAILED), DSL.inline(2))
                                            .when(
                                                DSL.inline(OperationStatus.SUCCEEDED),
                                                DSL.inline(0))
                                            .otherwise(DSL.inline(1))))
                                .from(ACTIVE_OPERATION)
                                .where(
                                    ACTIVE_OPERATION
                                        .WORKFLOW_RUN_ID
                                        .eq(WORKFLOW_RUN.ID)
                                        .and(
                                            ACTIVE_OPERATION.ATTEMPT.eq(
                                                ACTIVE_WORKFLOW_RUN.ATTEMPT)))),
                        DSL.inline(-1)))
                .when(DSL.inline(0), DSL.inline("SUCCEEDED"))
                .when(DSL.inline(2), DSL.inline("FAILED"))
                .when(DSL.inline(-1), DSL.inline("N/A"))
                .otherwise(DSL.inline("WAITING"))));
    STATUS_FIELDS.add(literalJsonEntry("created", WORKFLOW_RUN.CREATED));
    STATUS_FIELDS.add(literalJsonEntry("id", WORKFLOW_RUN.HASH_ID));
    STATUS_FIELDS.add(literalJsonEntry("inputFiles", WORKFLOW_RUN.INPUT_FILE_IDS));
    STATUS_FIELDS.add(literalJsonEntry("labels", WORKFLOW_RUN.LABELS));
    STATUS_FIELDS.add(literalJsonEntry("modified", WORKFLOW_RUN.MODIFIED));
    STATUS_FIELDS.add(literalJsonEntry("started", WORKFLOW_RUN.STARTED));
    STATUS_FIELDS.add(literalJsonEntry("arguments", WORKFLOW_RUN.ARGUMENTS));
    STATUS_FIELDS.add(literalJsonEntry("engineParameters", WORKFLOW_RUN.ENGINE_PARAMETERS));
    STATUS_FIELDS.add(literalJsonEntry("metadata", WORKFLOW_RUN.METADATA));
    STATUS_FIELDS.add(literalJsonEntry("waiting_resource", ACTIVE_WORKFLOW_RUN.WAITING_RESOURCE));
    STATUS_FIELDS.add(
        literalJsonEntry(
            "running",
            DSL.nvl(
                DSL.field(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE.ne(DSL.inline(Phase.FAILED))),
                DSL.inline(false))));
    STATUS_FIELDS.add(literalJsonEntry("attempt", ACTIVE_WORKFLOW_RUN.ATTEMPT));
    STATUS_FIELDS.add(
        literalJsonEntry(
            "enginePhase",
            DSL.case_(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE)
                .mapFields(
                    Stream.of(Phase.values())
                        .collect(Collectors.toMap(DSL::inline, v -> DSL.inline(v.name()))))));

    STATUS_FIELDS.add(literalJsonEntry("preflightOk", ACTIVE_WORKFLOW_RUN.PREFLIGHT_OKAY));
    STATUS_FIELDS.add(literalJsonEntry("target", ACTIVE_WORKFLOW_RUN.TARGET));
    STATUS_FIELDS.add(literalJsonEntry("tracing", ACTIVE_WORKFLOW_RUN.TRACING));
    STATUS_FIELDS.add(
        literalJsonEntry("consumableResources", ACTIVE_WORKFLOW_RUN.CONSUMABLE_RESOURCES));
    STATUS_FIELDS.add(literalJsonEntry("workflowRunUrl", ACTIVE_WORKFLOW_RUN.WORKFLOW_RUN_URL));
    STATUS_FIELDS.add(
        literalJsonEntry(
            "operations",
            DSL.field(
                DSL.select(
                        DSL.jsonArrayAgg(
                            DSL.jsonObject(
                                literalJsonEntry("attempt", ACTIVE_OPERATION.ATTEMPT),
                                literalJsonEntry(
                                    "enginePhase",
                                    DSL.case_(ACTIVE_OPERATION.ENGINE_PHASE)
                                        .mapFields(
                                            Stream.of(Phase.values())
                                                .collect(
                                                    Collectors.toMap(
                                                        DSL::inline, p -> DSL.inline(p.name()))))),
                                literalJsonEntry("error", ACTIVE_OPERATION.ERROR),
                                literalJsonEntry("recoveryState", ACTIVE_OPERATION.RECOVERY_STATE),
                                literalJsonEntry("debugInformation", ACTIVE_OPERATION.DEBUG_INFO),
                                literalJsonEntry("status", ACTIVE_OPERATION.STATUS),
                                literalJsonEntry("type", ACTIVE_OPERATION.TYPE))))
                    .from(ACTIVE_OPERATION)
                    .where(ACTIVE_OPERATION.WORKFLOW_RUN_ID.eq(WORKFLOW_RUN.ID)))));
  }

  private static Field<?> createQuery(VersionPolicy policy, Set<String> allowedTypes) {
    switch (policy) {
      case ALL:
        return createQueryOnVersion(
            externalVersionId -> DSL.jsonArrayAgg(externalVersionId.VALUE), allowedTypes);
      case LATEST:
        return createQueryOnVersion(
            externalVersionId ->
                DSL.lastValue(externalVersionId.VALUE)
                    .over()
                    .orderBy(externalVersionId.REQUESTED.desc()),
            allowedTypes);
      default:
        return DSL.inline(null, SQLDataType.JSON);
    }
  }

  private static Field<?> createQueryOnVersion(
      Function<ExternalIdVersion, Field<?>> fieldConstructor, Set<String> allowedTypes) {
    Condition condition = EXTERNAL_ID.ID.eq(EXTERNAL_ID_VERSION.EXTERNAL_ID_ID);
    if (allowedTypes != null && allowedTypes.size() != 0) {
      condition = condition.and(EXTERNAL_ID_VERSION.KEY.in(allowedTypes));
    }
    final ExternalIdVersion externalIdVersionAlias =
        EXTERNAL_ID_VERSION.as("externalIdVersionInner");
    final Table<Record1<String>> table =
        DSL.selectDistinct(EXTERNAL_ID_VERSION.KEY.as("desired_key"))
            .from(EXTERNAL_ID_VERSION)
            .where(condition)
            .asTable("keys");

    return DSL.field(
        DSL.select(
                DSL.jsonObjectAgg(
                    DSL.jsonEntry(
                        table.field(0, String.class),
                        DSL.field(
                            DSL.select(fieldConstructor.apply(externalIdVersionAlias))
                                .from(externalIdVersionAlias)
                                .where(
                                    externalIdVersionAlias
                                        .KEY
                                        .eq(table.field(0, String.class))
                                        .and(
                                            externalIdVersionAlias.EXTERNAL_ID_ID.eq(
                                                EXTERNAL_ID.ID)))
                                .limit(1)))))
            .from(table));
  }

  private static void handleException(HttpServerExchange exchange) {
    final Exception e = (Exception) exchange.getAttachment(ExceptionHandler.THROWABLE);
    e.printStackTrace();
    exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
    exchange.getResponseSender().send(e.getMessage());
  }

  private static <T> JSONEntry<T> literalJsonEntry(String key, Field<T> value) {
    return DSL.jsonEntry(DSL.inline(key), value);
  }

  public static void main(String[] args) throws IOException, SQLException {
    if (args.length != 1) {
      System.err.println(
          "Usage: java --module-path MODULES --module ca.on.oicr.gsi.vidarr.server"
              + " configuration.json");
    }
    DefaultExports.initialize();
    final Main main = new Main(MAPPER.readValue(new File(args[0]), ServerConfiguration.class));
    startServer(main);
  }

  static void startServer(Main server) throws SQLException {
    final PathHandler routes =
        Handlers.path(
            Handlers.routing()
                .get("/", monitor(new BlockingHandler(server::status)))
                .get("/api/file/{hash}", monitor(server::fetchFile))
                .get("/api/run/{hash}", monitor(new BlockingHandler(server::fetchRun)))
                .get("/api/recovery-failures", monitor(server::fetchRecoveryFailures))
                .get("/api/status", monitor(new BlockingHandler(server::fetchAllActive)))
                .get("/api/status/{hash}", monitor(server::fetchStatus))
                .get("/api/targets", monitor(server::fetchTargets))
                .get("/api/url/{hash}", monitor(server::fetchUrl))
                .get("/api/workflows", monitor(server::fetchWorkflows))
                .get("/api/max-in-flight", monitor(new BlockingHandler(server::fetchMaxInFlight)))
                .get("/metrics", monitor(new BlockingHandler(Main::metrics)))
                .post(
                    "/api/provenance",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(
                                MAPPER, AnalysisProvenanceRequest.class, server::fetchProvenance))))
                .post(
                    "/api/copy-out",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(MAPPER, UnloadRequest.class, server::copyOut))))
                .post(
                    "/api/unload",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(MAPPER, UnloadRequest.class, server::unload))))
                .post(
                    "/api/load",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(
                                MAPPER,
                                UnloadedData.class,
                                (exchange, data) -> server.load(exchange, data, true)))))
                .post(
                    "/api/load-injected",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(
                                MAPPER,
                                UnloadedData.class,
                                (exchange, data) -> server.load(exchange, data, false)))))
                .post(
                    "/api/retry-provision-out",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(
                                MAPPER,
                                RetryProvisionOutRequest.class,
                                server::retryProvisionOut))))
                .post("/api/reprovision-out",
                    monitor(new BlockingHandler(
                        JsonPost.parse(
                            MAPPER,
                            ReprovisionOutRequest.class,
                            server::reprovisionOut))))
                .delete(
                    "/api/status/{hash}", monitor(new BlockingHandler(server::deleteWorkflowRun)))
                .post(
                    "/api/submit",
                    monitor(JsonPost.parse(MAPPER, SubmitWorkflowRequest.class, server::submit)))
                .get("/api/workflow/{name}", monitor(new BlockingHandler(server::fetchWorkflow)))
                .post(
                    "/api/workflow/{name}",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(
                                MAPPER, AddWorkflowRequest.class, server::upsertWorkflow))))
                .delete(
                    "/api/workflow/{name}", monitor(new BlockingHandler(server::disableWorkflow)))
                .get(
                    "/api/workflow/{name}/{version}",
                    monitor(new BlockingHandler(server::fetchWorkflowVersion)))
                .post(
                    "/api/workflow/{name}/{version}",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(
                                MAPPER,
                                AddWorkflowVersionRequest.class,
                                server::addWorkflowVersion))))
                .post(
                    "/api/versions",
                    monitor(
                        new BlockingHandler(
                            JsonPost.parse(
                                MAPPER, BulkVersionRequest.class, server::updateVersions))))
                .setFallbackHandler(
                    new ResourceHandler(
                        new ClassPathResourceManager(
                            server.getClass().getClassLoader(), server.getClass().getPackage()))));
    for (final Entry<String, HttpHandler> consumableResource :
        server.consumableResources.entrySet()) {
      routes.addPrefixPath(
          "/consumable-resource/" + consumableResource.getKey(), consumableResource.getValue());
    }
    routes.addPrefixPath(
        "/consumable-resource/max-in-flight-by-workflow",
        server.overridableMaxInFlightPerWorkflow.httpHandler().get());
    final Undertow undertow =
        Undertow.builder()
            .addHttpListener(server.port, "0.0.0.0")
            .setWorkerThreads(server.dataSource.getMaximumPoolSize())
            .setHandler(
                Handlers.exceptionHandler(routes)
                    .addExceptionHandler(Exception.class, Main::handleException))
            .build();
    undertow.start();
    server.recover();
  }

  private void reprovisionOut(HttpServerExchange exchange,
      ReprovisionOutRequest reprovisionOutRequest) {
    if (!reprovisionOutRequest.check()) {
      // TODO error msg
      exchange.setStatusCode(400);
      return;
    }
    String provisionerName = reprovisionOutRequest.getOutputProvisionerName();
    OutputProvisioner<?, ?> provisioner = outputProvisioners.get(
        provisionerName);
    if(null == provisioner){
      // TODO error msg
      exchange.setStatusCode(400);
      return;
    }
    final AtomicReference<Runnable> postCommitAction = new AtomicReference<>();
    final Pair<Integer, SubmitWorkflowResponse> response = processor.reprovisionOut(
        reprovisionOutRequest.getWorkflowRunHashId(), provisionerName, provisioner,
        reprovisionOutRequest.getOutputPath(),
        new DatabaseBackedProcessor.SubmissionResultHandler<>() {
          @Override
          public boolean allowLaunch() {
            return true;
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> dryRunResult() {
            return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseDryRun());
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> externalIdMismatch(String error) {
            return new Pair<>(
                StatusCodes.BAD_REQUEST,
                new SubmitWorkflowResponseFailure("External IDs do not match: " + error));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> internalError(Exception e) {
            e.printStackTrace();
            return new Pair<>(
                StatusCodes.INTERNAL_SERVER_ERROR,
                new SubmitWorkflowResponseFailure(e.getMessage()));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> invalidWorkflow(Set<String> errors) {
            return new Pair<>(
                StatusCodes.BAD_REQUEST, new SubmitWorkflowResponseFailure(errors));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> launched(
              String vidarrId, Runnable start) {
            postCommitAction.set(start);
            return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseSuccess(vidarrId));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> matchExisting(String vidarrId) {
            return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseSuccess(vidarrId));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> missingExternalIdVersion() {
            return new Pair<>(
                StatusCodes.BAD_REQUEST,
                new SubmitWorkflowResponseFailure("External IDs do not have versions set."));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> missingExternalKeyVersions(
              String vidarrId, List<ExternalKey> missingKeys) {
            return new Pair<>(
                StatusCodes.BAD_REQUEST,
                new SubmitWorkflowResponseMissingKeyVersions(vidarrId, missingKeys));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> multipleMatches(List<String> matchIds) {
            return new Pair<>(
                StatusCodes.CONFLICT, new SubmitWorkflowResponseConflict(matchIds));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> reinitialise(
              String vidarrId, Runnable start) {
            postCommitAction.set(start);
            return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseSuccess(vidarrId));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> unknownTarget(String targetName) {
            return new Pair<>(
                StatusCodes.BAD_REQUEST,
                new SubmitWorkflowResponseFailure(
                    String.format("Target %s is unknown", targetName)));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> unknownWorkflow(
              String name, String version) {
            return new Pair<>(
                StatusCodes.BAD_REQUEST,
                new SubmitWorkflowResponseFailure(
                    String.format("Workflow %s (%s) is unknown", name, version)));
          }

          @Override
          public Pair<Integer, SubmitWorkflowResponse> unresolvedIds(TreeSet<String> inputId) {
            return new Pair<>(
                StatusCodes.BAD_REQUEST,
                new SubmitWorkflowResponseFailure(
                    inputId.stream()
                        .map(id -> String.format("Input ID %s cannot be resolved", id))
                        .collect(Collectors.toList())));
          }
        });
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
    exchange.setStatusCode(response.first());
    if (postCommitAction.get() != null) {
      postCommitAction.get().run();
    }
    try {
      exchange.getResponseSender().send(MAPPER.writeValueAsString(response.second()));
    } catch (JsonProcessingException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private static void metrics(HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, TextFormat.CONTENT_TYPE_004);
    exchange.setStatusCode(StatusCodes.OK);
    try (final OutputStream os = exchange.getOutputStream();
        final PrintWriter writer = new PrintWriter(os)) {
      TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static HttpHandler monitor(HttpHandler handler) {
    return exchange -> {
      final PathTemplateMatch url = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
      try (final AutoCloseable ignored =
          RESPONSE_TIME.start(url == null ? "unknown" : url.getMatchedTemplate())) {
        handler.handleRequest(exchange);
      }
    };
  }

  private boolean canSubmit = true;
  private final Map<String, HttpHandler> consumableResources = new TreeMap<>();
  private final HikariDataSource dataSource;
  private long epoch = ManagementFactory.getRuntimeMXBean().getStartTime();
  private final ReentrantReadWriteLock epochLock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
  private final Map<String, InputProvisioner<?>> inputProvisioners;
  private final Semaphore loadCounter = new Semaphore(3);
  private final MaxInFlightByWorkflow maxInFlightPerWorkflow = new MaxInFlightByWorkflow();
  private final ManualOverrideConsumableResource overridableMaxInFlightPerWorkflow =
      new ManualOverrideConsumableResource();
  private final Map<String, String> otherServers;
  private final Map<String, OutputProvisioner<?, ?>> outputProvisioners;
  private final int port;
  private final DatabaseBackedProcessor processor;
  private final Map<String, RuntimeProvisioner<?>> runtimeProvisioners;
  private final String selfName;
  private final String selfUrl;
  private final Map<String, Target> targets;
  private final Path unloadDirectory;
  private final Map<String, WorkflowEngine<?, ?>> workflowEngines;
  private final StatusPage status =
      new StatusPage(this) {
        @Override
        protected void emitCore(SectionRenderer sectionRenderer) {
          sectionRenderer.line("Self-URL", selfUrl);
          for (final String target : targets.keySet()) {
            sectionRenderer.line("Target", target);
          }
        }

        @Override
        public Stream<ConfigurationSection> sections() {
          return Stream.of(
                  workflowEngines.entrySet().stream()
                      .map(
                          e ->
                              new ConfigurationSection("Workflow Engine: " + e.getKey()) {
                                @Override
                                public void emit(SectionRenderer sectionRenderer)
                                    throws XMLStreamException {
                                  e.getValue().configuration(sectionRenderer);
                                }
                              }),
                  inputProvisioners.entrySet().stream()
                      .map(
                          e ->
                              new ConfigurationSection("Input Provisioner: " + e.getKey()) {
                                @Override
                                public void emit(SectionRenderer sectionRenderer)
                                    throws XMLStreamException {
                                  e.getValue().configuration(sectionRenderer);
                                }
                              }),
                  outputProvisioners.entrySet().stream()
                      .map(
                          e ->
                              new ConfigurationSection("Output Provisioner: " + e.getKey()) {
                                @Override
                                public void emit(SectionRenderer sectionRenderer)
                                    throws XMLStreamException {
                                  e.getValue().configuration(sectionRenderer);
                                }
                              }),
                  runtimeProvisioners.entrySet().stream()
                      .map(
                          e ->
                              new ConfigurationSection("Runtime Provisioner: " + e.getKey()) {
                                @Override
                                public void emit(SectionRenderer sectionRenderer)
                                    throws XMLStreamException {
                                  e.getValue().configuration(sectionRenderer);
                                }
                              }))
              .flatMap(Function.identity());
        }
      };

  Main(ServerConfiguration configuration) throws SQLException {
    canSubmit = configuration.getCanSubmit();
    selfUrl = configuration.getUrl();
    selfName = configuration.getName();
    port = configuration.getPort();
    otherServers = configuration.getOtherServers();
    workflowEngines = configuration.getWorkflowEngines();
    inputProvisioners = configuration.getInputProvisioners();
    outputProvisioners = configuration.getOutputProvisioners();
    runtimeProvisioners = configuration.getRuntimeProvisioners();

    for (final InputProvisioner<?> input : inputProvisioners.values()) {
      input.startup();
    }
    for (final OutputProvisioner<?, ?> output : outputProvisioners.values()) {
      output.startup();
    }
    for (final RuntimeProvisioner<?> runtime : runtimeProvisioners.values()) {
      runtime.startup();
    }
    for (final WorkflowEngine<?, ?> engine : workflowEngines.values()) {
      engine.startup();
    }
    final Map<String, ConsumableResource> consumableResources =
        configuration.getConsumableResources();
    for (final Entry<String, ConsumableResource> resource : consumableResources.entrySet()) {
      resource.getValue().startup(resource.getKey());
      resource
          .getValue()
          .httpHandler()
          .ifPresent(handler -> this.consumableResources.put(resource.getKey(), handler));
    }
    targets =
        configuration.getTargets().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        new Target() {
                          private final List<Pair<String, ConsumableResource>> consumables =
                              Stream.concat(
                                      e.getValue().getConsumableResources().stream()
                                          .map(
                                              name ->
                                                  new Pair<>(name, consumableResources.get(name))),
                                      Stream.of(
                                          new Pair<String, ConsumableResource>(
                                              "", overridableMaxInFlightPerWorkflow)))
                                  .toList();
                          private final WorkflowEngine<?, ?> engine =
                              workflowEngines.get(e.getValue().getWorkflowEngine());
                          private final Map<InputProvisionFormat, InputProvisioner<?>>
                              inputProvisioners =
                                  e.getValue().getInputProvisioners().stream()
                                      .map(Main.this.inputProvisioners::get)
                                      .flatMap(
                                          p ->
                                              Stream.of(InputProvisionFormat.values())
                                                  .filter(p::canProvision)
                                                  .map(
                                                      f ->
                                                          new Pair<
                                                              InputProvisionFormat,
                                                              InputProvisioner<?>>(f, p)))
                                      .collect(
                                          Collectors
                                              .<Pair<InputProvisionFormat, InputProvisioner<?>>,
                                                  InputProvisionFormat, InputProvisioner<?>>
                                                  toMap(Pair::first, Pair::second));
                          private final Map<OutputProvisionFormat, OutputProvisioner<?, ?>>
                              outputProvisioners =
                                  e.getValue().getOutputProvisioners().stream()
                                      .map(Main.this.outputProvisioners::get)
                                      .flatMap(
                                          p ->
                                              Stream.of(OutputProvisionFormat.values())
                                                  .filter(p::canProvision)
                                                  .map(
                                                      f ->
                                                          new Pair<
                                                              OutputProvisionFormat,
                                                              OutputProvisioner<?, ?>>(f, p)))
                                      .collect(
                                          Collectors
                                              .<Pair<
                                                      OutputProvisionFormat,
                                                      OutputProvisioner<?, ?>>,
                                                  OutputProvisionFormat, OutputProvisioner<?, ?>>
                                                  toMap(Pair::first, Pair::second));
                          private final List<RuntimeProvisioner<?>> runtimeProvisioners =
                              e.getValue().getRuntimeProvisioners().stream()
                                  .<RuntimeProvisioner<?>>map(Main.this.runtimeProvisioners::get)
                                  .toList();

                          @Override
                          public Stream<Pair<String, ConsumableResource>> consumableResources() {
                            return consumables.stream();
                          }

                          @Override
                          public WorkflowEngine<?, ?> engine() {
                            return engine;
                          }

                          @Override
                          public InputProvisioner<?> provisionerFor(InputProvisionFormat type) {
                            return inputProvisioners.get(type);
                          }

                          @Override
                          public OutputProvisioner<?, ?> provisionerFor(
                              OutputProvisionFormat type) {
                            return outputProvisioners.get(type);
                          }

                          @Override
                          public Stream<RuntimeProvisioner<?>> runtimeProvisioners() {
                            return runtimeProvisioners.stream();
                          }
                        }));

    final PGSimpleDataSource simpleConnection = new PGSimpleDataSource();
    simpleConnection.setServerNames(new String[] {configuration.getDbHost()});
    simpleConnection.setPortNumbers(new int[] {configuration.getDbPort()});
    simpleConnection.setDatabaseName(configuration.getDbName());
    simpleConnection.setUser(configuration.getDbUser());
    simpleConnection.setPassword(configuration.getDbPass());
    FluentConfiguration fw = Flyway.configure().dataSource(simpleConnection);
    fw.locations("classpath:db/migration").load().migrate();

    final HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        String.format(
            "jdbc:postgresql://%s:%d/%s",
            configuration.getDbHost(), configuration.getDbPort(), configuration.getDbName()));
    config.setUsername(configuration.getDbUser());
    config.setPassword(configuration.getDbPass());
    config.setAutoCommit(false);
    config.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
    dataSource = new HikariDataSource(config);
    // This limit is selected because of the default maximum number of connections supported by
    // Postgres
    dataSource.setMaximumPoolSize(Math.min(10 * Runtime.getRuntime().availableProcessors(), 95));
    processor =
        new DatabaseBackedProcessor(executor, dataSource) {
          private Optional<FileMetadata> fetchPathForId(String id) {
            final Matcher match = BaseProcessor.ANALYSIS_RECORD_ID.matcher(id);
            if (!match.matches() || !match.group("type").equals("file")) {
              return Optional.empty();
            }
            final String instance = match.group("instance");
            final String hash = match.group("hash");
            if (instance.equals("_") || instance.equals(selfName)) {
              return resolveInDatabase(hash);
            }
            final String remote = otherServers.get(instance);
            if (remote == null) {
              return Optional.empty();
            }

            try (final AutoCloseable ignored = REMOTE_RESPONSE_TIME.start(remote)) {
              final HttpResponse<Supplier<ProvenanceAnalysisRecord<ExternalMultiVersionKey>>>
                  response =
                      CLIENT.send(
                          HttpRequest.newBuilder()
                              .uri(URI.create(String.format("%s/api/file/%s", remote, hash)))
                              .timeout(Duration.ofMinutes(1))
                              .GET()
                              .build(),
                          new JsonBodyHandler<>(MAPPER, new TypeReference<>() {}));
              if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                final ProvenanceAnalysisRecord<ExternalMultiVersionKey> result =
                    response.body().get();
                return Optional.of(
                    new FileMetadata() {
                      private final List<ExternalMultiVersionKey> keys = result.getExternalKeys();
                      private final String path = result.getPath();

                      @Override
                      public Stream<ExternalMultiVersionKey> externalKeys() {
                        return keys.stream();
                      }

                      @Override
                      public String path() {
                        return path;
                      }
                    });
              } else {
                return Optional.empty();
              }

            } catch (Exception e) {
              e.printStackTrace();
              REMOTE_ERROR_COUNT.labels(remote).inc();
              return Optional.empty();
            }
          }

          @Override
          public Optional<FileMetadata> pathForId(String id) {
            return this.fetchPathForId(id);
          }

          @Override
          protected Optional<Target> targetByName(String name) {
            return Optional.ofNullable(targets.get(name));
          }
        };

    try (final Connection connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .select(WORKFLOW.NAME, WORKFLOW.MAX_IN_FLIGHT)
          .from(WORKFLOW)
          .forEach(record -> maxInFlightPerWorkflow.set(record.value1(), record.value2()));
    }
    overridableMaxInFlightPerWorkflow.setInner(maxInFlightPerWorkflow);
    unloadDirectory = Path.of(configuration.getUnloadDirectory());
  }

  private void upsertWorkflow(HttpServerExchange exchange, AddWorkflowRequest request) {
    final String name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");

    try (final Connection connection = dataSource.getConnection()) {
      final JSONB labels = JSONB.valueOf(MAPPER.writeValueAsString(request.getLabels()));
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              context ->
                  DSL.using(context)
                      .insertInto(WORKFLOW)
                      .set(WORKFLOW.NAME, param("workflowName", name))
                      .set(WORKFLOW.LABELS, param("labels", labels))
                      .set(WORKFLOW.IS_ACTIVE, param("isActive", true))
                      .set(WORKFLOW.MAX_IN_FLIGHT, param("maxInFlight", request.getMaxInFlight()))
                      .onConflict(WORKFLOW.NAME)
                      .doUpdate()
                      .set(WORKFLOW.IS_ACTIVE, true)
                      .set(WORKFLOW.MAX_IN_FLIGHT, request.getMaxInFlight())
                      .where(
                          WORKFLOW
                              .IS_ACTIVE
                              .isFalse()
                              .or(WORKFLOW.MAX_IN_FLIGHT.ne(request.getMaxInFlight())))
                      .execute());
      maxInFlightPerWorkflow.set(name, request.getMaxInFlight());
      okEmptyResponse(exchange);
    } catch (SQLException | JsonProcessingException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void addWorkflowVersion(HttpServerExchange exchange, AddWorkflowVersionRequest request) {
    final String name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");
    final String version =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("version");
    List<ValidationError> validationErrors = new ArrayList<>();
    if (request.getParameters() == null || request.getParameters().isEmpty()) {
      validationErrors.add(new ValidationError("parameters", "No parameter types found"));
    }
    if (request.getOutputs() == null || request.getOutputs().isEmpty()) {
      validationErrors.add(new ValidationError("outputs", "No output types found"));
    }
    if (request.getWorkflow() == null
        || "".equals(request.getWorkflow())
        || "string".equals(request.getWorkflow())) {
      // A string containing the actual workflow commands that get run
      validationErrors.add(
          new ValidationError(
              "workflow", "A workflow definition/workflow file string is required"));
    }
    if (request.getLanguage() == null) {
      validationErrors.add(ValidationError.forRequired("language"));
    }
    if (!validationErrors.isEmpty()) {
      badRequestResponse(exchange, validationErrors.toString());
      return;
    }
    try (final Connection connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              context -> {
                final DSLContext dsl = DSL.using(context);
                Optional<Integer> matchingWorkflow =
                    dsl.select(WORKFLOW.ID)
                        .from(WORKFLOW)
                        .where(WORKFLOW.NAME.eq(param("workflowName", name)))
                        .fetchOptional(Record1::value1);
                if (matchingWorkflow.isEmpty()) {
                  notFoundResponse(exchange, String.format("No workflow with name %s found", name));
                  return;
                }
                Optional<Record1<String>> matchingWorkflowVersionHash =
                    dsl.select(WORKFLOW_VERSION.HASH_ID)
                        .from(WORKFLOW_VERSION)
                        .where(
                            WORKFLOW_VERSION
                                .NAME
                                .eq(param("workflowName", name))
                                .and(
                                    WORKFLOW_VERSION.VERSION.eq(param("workflowVersion", version))))
                        .fetchOptional();
                if (matchingWorkflowVersionHash.isPresent()) {
                  // Never modify an existing workflow version. It's ok if the submitted workflow
                  // version matches the existing workflow version, but return an error if they
                  // are different.
                  final String definitionHash =
                      generateWorkflowDefinitionHash(request.getWorkflow());
                  final TreeMap<String, String> accessoryHashes =
                      generateAccessoryWorkflowHashes(request.getAccessoryFiles());
                  final String versionHash =
                      generateWorkflowVersionHash(
                          name,
                          version,
                          definitionHash,
                          request.getOutputs(),
                          request.getParameters(),
                          accessoryHashes);
                  String existingWorkflowVersionHash = matchingWorkflowVersionHash.get().value1();
                  if (existingWorkflowVersionHash.equals(versionHash)) {
                    okEmptyResponse(exchange);
                  } else {
                    conflictResponse(exchange);
                  }
                  return;
                }
                final String definitionHash = generateWorkflowDefinitionHash(request.getWorkflow());
                insertWorkflowDefinition(
                    dsl, request.getLanguage(), request.getWorkflow(), definitionHash);
                final TreeMap<String, String> accessoryHashes =
                    generateAccessoryWorkflowHashes(request.getAccessoryFiles());
                insertWorkflowAccessoryFiles(
                    request.getAccessoryFiles(), accessoryHashes, request.getLanguage(), dsl);
                final String versionHash =
                    generateWorkflowVersionHash(
                        name,
                        version,
                        definitionHash,
                        request.getOutputs(),
                        request.getParameters(),
                        accessoryHashes);
                final Optional<Record1<Boolean>> result =
                    dsl.insertInto(WORKFLOW_VERSION)
                        .set(WORKFLOW_VERSION.HASH_ID, param("hashId", versionHash))
                        .set(
                            WORKFLOW_VERSION.METADATA,
                            DSL.val(
                                MAPPER.valueToTree(request.getOutputs()),
                                WORKFLOW_VERSION.METADATA.getDataType()))
                        .set(WORKFLOW_VERSION.NAME, param("workflowName", name))
                        .set(
                            WORKFLOW_VERSION.PARAMETERS,
                            DSL.val(
                                MAPPER.valueToTree(request.getParameters()),
                                WORKFLOW_VERSION.PARAMETERS.getDataType()))
                        .set(
                            WORKFLOW_VERSION.WORKFLOW_DEFINITION,
                            DSL.field(
                                DSL.select(WORKFLOW_DEFINITION.ID)
                                    .from(WORKFLOW_DEFINITION)
                                    .where(
                                        WORKFLOW_DEFINITION.HASH_ID.eq(
                                            param("definitionHash", definitionHash)))))
                        .set(WORKFLOW_VERSION.VERSION, param("version", version))
                        .onConflict(WORKFLOW_VERSION.NAME, WORKFLOW_VERSION.VERSION)
                        .doNothing()
                        .returningResult(
                            DSL.field(
                                WORKFLOW_VERSION.HASH_ID.eq(param("versionHash", versionHash))))
                        .fetchOptional();
                if (result.map(r -> !r.value1()).orElse(false)) {
                  conflictResponse(exchange);
                  return;
                }
                dsl.update(WORKFLOW)
                    .set(WORKFLOW.IS_ACTIVE, true)
                    .where(
                        WORKFLOW
                            .NAME
                            .eq(param("workflowName", name))
                            .and(WORKFLOW.IS_ACTIVE.isFalse()))
                    .execute();
                if (!accessoryHashes.isEmpty()) {

                  InsertValuesStep3<WorkflowVersionAccessoryRecord, Integer, String, Integer>
                      accessoryQuery =
                          dsl.insertInto(
                              WORKFLOW_VERSION_ACCESSORY,
                              WORKFLOW_VERSION_ACCESSORY.WORKFLOW_VERSION,
                              WORKFLOW_VERSION_ACCESSORY.FILENAME,
                              WORKFLOW_VERSION_ACCESSORY.WORKFLOW_DEFINITION);

                  for (final Entry<String, String> accessory : accessoryHashes.entrySet()) {
                    accessoryQuery =
                        accessoryQuery.values(
                            DSL.field(
                                DSL.select(WORKFLOW_VERSION.ID)
                                    .from(WORKFLOW_VERSION)
                                    .where(
                                        WORKFLOW_VERSION
                                            .NAME
                                            .eq(param("workflowName", name))
                                            .and(
                                                WORKFLOW_VERSION.VERSION.eq(
                                                    param("workflowVersion", version))))),
                            DSL.val(accessory.getKey()),
                            DSL.field(
                                DSL.select(WORKFLOW_DEFINITION.ID)
                                    .from(WORKFLOW_DEFINITION)
                                    .where(
                                        WORKFLOW_DEFINITION.HASH_ID.eq(
                                            param("hashId", accessory.getValue())))));
                  }
                  accessoryQuery.execute();
                }
                createdResponse(exchange);
              });
    } catch (SQLException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void associateDefinitionAsAccessory(
      Configuration configuration, int id, String fileName, String accessoryWorkflowHash) {
    DSL.using(configuration)
        .insertInto(WORKFLOW_VERSION_ACCESSORY)
        .set(WORKFLOW_VERSION_ACCESSORY.FILENAME, param("filename", fileName))
        .set(
            WORKFLOW_VERSION_ACCESSORY.WORKFLOW_DEFINITION,
            DSL.select(WORKFLOW_DEFINITION.ID)
                .from(WORKFLOW_DEFINITION)
                .where(
                    WORKFLOW_DEFINITION.HASH_ID.eq(
                        param("accessoryWorkflowHash", accessoryWorkflowHash))))
        .set(WORKFLOW_VERSION_ACCESSORY.WORKFLOW_VERSION, param("workflowVersion", id))
        .execute();
  }

  private void copyOut(HttpServerExchange exchange, UnloadRequest request) {
    try {
      unloadSearch(
          request,
          (tx, ids) -> {
            exchange.setStatusCode(StatusCodes.OK);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
            try (final JsonGenerator output =
                MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
              dumpUnloadDataToJson(tx, ids, output);
            }
            return null;
          });
    } catch (Exception e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private Field<JSON> createAnalysisJsonField(Field<JSON> externalKeys, JSONEntry<?>... extra) {
    final ArrayList<JSONEntry<?>> analysisCommonFields = new ArrayList<>(List.of(extra));
    analysisCommonFields.add(literalJsonEntry("id", ANALYSIS.HASH_ID));
    analysisCommonFields.add(literalJsonEntry("type", ANALYSIS.ANALYSIS_TYPE));
    analysisCommonFields.add(literalJsonEntry("created", ANALYSIS.CREATED));
    analysisCommonFields.add(literalJsonEntry("labels", ANALYSIS.LABELS));
    analysisCommonFields.add(literalJsonEntry("modified", ANALYSIS.MODIFIED));
    analysisCommonFields.add(literalJsonEntry("externalKeys", externalKeys));

    final ArrayList<JSONEntry<?>> analysisFileFields = new ArrayList<>(analysisCommonFields);
    analysisFileFields.add(literalJsonEntry("path", ANALYSIS.FILE_PATH));
    analysisFileFields.add(literalJsonEntry("checksum", ANALYSIS.FILE_CHECKSUM));
    analysisFileFields.add(literalJsonEntry("checksumType", ANALYSIS.FILE_CHECKSUM_TYPE));
    analysisFileFields.add(literalJsonEntry("metatype", ANALYSIS.FILE_METATYPE));
    analysisFileFields.add(literalJsonEntry("size", ANALYSIS.FILE_SIZE));

    final ArrayList<JSONEntry<?>> analysisUrlFields = new ArrayList<>(analysisCommonFields);
    analysisUrlFields.add(literalJsonEntry("url", ANALYSIS.FILE_PATH));

    return DSL.case_(ANALYSIS.ANALYSIS_TYPE)
        .when("file", DSL.jsonObject(analysisFileFields))
        .when("url", DSL.jsonObject(analysisUrlFields));
  }

  private void createAnalysisRecords(
      DSLContext context,
      JsonGenerator jsonGenerator,
      VersionPolicy policy,
      Set<String> allowedTypes,
      boolean includeParameters,
      Set<AnalysisOutputType> includedAnalyses,
      Condition condition)
      throws SQLException {
    final ArrayList<JSONEntry<?>> fields = new ArrayList<>();

    fields.add(literalJsonEntry("completed", WORKFLOW_RUN.COMPLETED));
    fields.add(literalJsonEntry("created", WORKFLOW_RUN.CREATED));
    fields.add(literalJsonEntry("id", WORKFLOW_RUN.HASH_ID));
    fields.add(literalJsonEntry("inputFiles", WORKFLOW_RUN.INPUT_FILE_IDS));
    fields.add(literalJsonEntry("labels", WORKFLOW_RUN.LABELS));
    fields.add(literalJsonEntry("modified", WORKFLOW_RUN.MODIFIED));
    fields.add(literalJsonEntry("started", WORKFLOW_RUN.STARTED));
    fields.add(literalJsonEntry("lastAccessed", WORKFLOW_RUN.LAST_ACCESSED));
    fields.add(literalJsonEntry("instanceName", DSL.coalesce(selfName, DSL.inline(selfName))));

    if (includeParameters) {
      fields.add(literalJsonEntry("arguments", WORKFLOW_RUN.ARGUMENTS));
      fields.add(literalJsonEntry("engineParameters", WORKFLOW_RUN.ENGINE_PARAMETERS));
      fields.add(literalJsonEntry("metadata", WORKFLOW_RUN.METADATA));
    }
    fields.add(
        literalJsonEntry(
            "workflowName",
            DSL.field(
                DSL.select(WORKFLOW_VERSION.NAME)
                    .from(WORKFLOW_VERSION)
                    .where(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID)))));
    fields.add(
        literalJsonEntry(
            "workflowVersion",
            DSL.field(
                DSL.select(WORKFLOW_VERSION.VERSION)
                    .from(WORKFLOW_VERSION)
                    .where(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID)))));

    fields.add(
        literalJsonEntry(
            "externalKeys",
            DSL.field(
                DSL.select(
                        DSL.jsonArrayAgg(
                            DSL.jsonObject(
                                literalJsonEntry("id", EXTERNAL_ID.EXTERNAL_ID_),
                                literalJsonEntry("provider", EXTERNAL_ID.PROVIDER),
                                literalJsonEntry("created", EXTERNAL_ID.CREATED),
                                literalJsonEntry("modified", EXTERNAL_ID.MODIFIED),
                                literalJsonEntry("requested", EXTERNAL_ID.REQUESTED),
                                literalJsonEntry("versions", createQuery(policy, allowedTypes)))))
                    .from(EXTERNAL_ID)
                    .where(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(WORKFLOW_RUN.ID)))));

    fields.add(
        literalJsonEntry(
            "analysis",
            DSL.field(
                DSL.select(
                        DSL.jsonArrayAgg(
                            createAnalysisJsonField(
                                DSL.field(
                                    DSL.select(
                                            DSL.jsonArrayAgg(
                                                DSL.jsonObject(
                                                    literalJsonEntry(
                                                        "provider", EXTERNAL_ID.PROVIDER),
                                                    literalJsonEntry(
                                                        "id", EXTERNAL_ID.EXTERNAL_ID_))))
                                        .from(
                                            EXTERNAL_ID
                                                .join(ANALYSIS_EXTERNAL_ID)
                                                .on(
                                                    ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID.eq(
                                                        EXTERNAL_ID.ID)))
                                        .where(ANALYSIS_EXTERNAL_ID.ANALYSIS_ID.eq(ANALYSIS.ID))))))
                    .from(ANALYSIS)
                    .where(
                        ANALYSIS
                            .WORKFLOW_RUN_ID
                            .eq(WORKFLOW_RUN.ID)
                            .and(
                                ANALYSIS.ANALYSIS_TYPE.in(
                                    includedAnalyses.stream()
                                        .map(
                                            t ->
                                                switch (t) {
                                                  case FILE -> "file";
                                                  case URL -> "url";
                                                })
                                        .collect(Collectors.toList())))))));

    context
        .select(DSL.jsonObject(fields))
        .from(WORKFLOW_RUN)
        .where(condition)
        .forEach(
            result -> {
              try {
                jsonGenerator.writeRawValue(result.value1().data());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private void deleteWorkflowRun(HttpServerExchange exchange) {
    final String vidarrId =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("hash");
    exchange.setStatusCode(
        processor.delete(
            vidarrId,
            new DeleteResultHandler<>() {
              @Override
              public Integer deleted() {
                return StatusCodes.OK;
              }

              @Override
              public Integer internalError(SQLException e) {
                return StatusCodes.INTERNAL_SERVER_ERROR;
              }

              @Override
              public Integer noWorkflowRun() {
                return StatusCodes.NOT_FOUND;
              }

              @Override
              public Integer stillActive() {
                return StatusCodes.CONFLICT;
              }
            }));
    exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
    exchange.getResponseSender().send("");
  }

  private void disableWorkflow(HttpServerExchange exchange) {
    final String name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");

    try (final Connection connection = dataSource.getConnection()) {
      final Integer count =
          DSL.using(connection, SQLDialect.POSTGRES)
              .transactionResult(
                  context ->
                      DSL.using(context)
                          .update(WORKFLOW)
                          .set(WORKFLOW.IS_ACTIVE, false)
                          .where(
                              WORKFLOW
                                  .NAME
                                  .eq(param("workflowName", name))
                                  .and(WORKFLOW.IS_ACTIVE.isTrue()))
                          .execute());
      if (count == 0) {
        notFoundResponse(exchange);
      } else {
        okEmptyResponse(exchange);
      }
    } catch (SQLException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void dumpUnloadDataToJson(Configuration tx, Long[] ids, JsonGenerator output)
      throws IOException, SQLException {
    output.writeStartObject();
    output.writeArrayFieldStart("workflows");
    DSL.using(tx)
        .select(
            DSL.jsonObject(
                literalJsonEntry("name", WORKFLOW.NAME),
                literalJsonEntry("labels", WORKFLOW.LABELS)))
        .from(WORKFLOW)
        .where(
            DSL.exists(
                DSL.select()
                    .from(
                        WORKFLOW_RUN
                            .join(WORKFLOW_VERSION)
                            .on(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID)))
                    .where(
                        WORKFLOW_RUN
                            .ID
                            .eq(DSL.any(ids))
                            .and(WORKFLOW_VERSION.NAME.eq(WORKFLOW.NAME)))))
        .forEach(
            result -> {
              try {
                output.writeRawValue(result.value1().data());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    output.writeEndArray();
    output.writeArrayFieldStart("workflowVersions");
    final WorkflowDefinition accessoryDefinition =
        WORKFLOW_DEFINITION.as("accessoryWorkflowDefinition");
    DSL.using(tx)
        .select(
            DSL.jsonObject(
                literalJsonEntry(
                    "accessoryFiles",
                    DSL.coalesce(
                        DSL.field(
                            DSL.select(
                                    DSL.jsonObjectAgg(
                                        WORKFLOW_VERSION_ACCESSORY.FILENAME,
                                        accessoryDefinition.WORKFLOW_FILE))
                                .from(
                                    WORKFLOW_VERSION_ACCESSORY
                                        .join(accessoryDefinition)
                                        .on(
                                            accessoryDefinition.ID.eq(
                                                WORKFLOW_VERSION_ACCESSORY.WORKFLOW_DEFINITION))
                                        .where(
                                            WORKFLOW_VERSION_ACCESSORY.WORKFLOW_VERSION.eq(
                                                WORKFLOW_VERSION.ID)))),
                        JSON.json("{}"))),
                literalJsonEntry("language", WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE),
                literalJsonEntry("name", WORKFLOW_VERSION.NAME),
                literalJsonEntry("outputs", WORKFLOW_VERSION.METADATA),
                literalJsonEntry("parameters", WORKFLOW_VERSION.PARAMETERS),
                literalJsonEntry("version", WORKFLOW_VERSION.VERSION),
                literalJsonEntry("workflow", WORKFLOW_DEFINITION.WORKFLOW_FILE)))
        .from(
            WORKFLOW_VERSION
                .join(WORKFLOW_DEFINITION)
                .on(WORKFLOW_VERSION.WORKFLOW_DEFINITION.eq(WORKFLOW_DEFINITION.ID)))
        .where(
            DSL.exists(
                DSL.select()
                    .from(WORKFLOW_RUN)
                    .where(
                        WORKFLOW_RUN
                            .ID
                            .in(ids)
                            .and(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID)))))
        .forEach(
            result -> {
              try {
                output.writeRawValue(result.value1().data());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

    output.writeEndArray();
    output.writeArrayFieldStart("workflowRuns");
    createAnalysisRecords(
        DSL.using(tx),
        output,
        VersionPolicy.ALL,
        null,
        true,
        EnumSet.allOf(AnalysisOutputType.class),
        WORKFLOW_RUN.ID.in(ids));
    output.writeEndArray();
    output.writeEndObject();
  }

  private void fetchAllActive(HttpServerExchange exchange) {

    try (final Connection connection = dataSource.getConnection();
        final JsonGenerator output = MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
      exchange.setStatusCode(StatusCodes.OK);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
      output.writeStartArray();
      DSL.using(connection, SQLDialect.POSTGRES)
          .select(DSL.jsonObject(STATUS_FIELDS))
          .from(
              WORKFLOW_RUN
                  .leftJoin(ACTIVE_WORKFLOW_RUN)
                  .on(WORKFLOW_RUN.ID.eq(ACTIVE_WORKFLOW_RUN.ID)))
          .where(ACTIVE_WORKFLOW_RUN.ID.isNotNull())
          .forEach(
              result -> {
                try {
                  output.writeRawValue(result.value1().data());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
      output.writeEndArray();
    } catch (SQLException | IOException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void fetchAnalysis(HttpServerExchange exchange, String type) {
    final String vidarrId =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("hash");
    try (final Connection connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .select(
              createAnalysisJsonField(
                  DSL.field(
                      DSL.select(
                              DSL.jsonArrayAgg(
                                  DSL.jsonObject(
                                      literalJsonEntry("id", EXTERNAL_ID.EXTERNAL_ID_),
                                      literalJsonEntry("provider", EXTERNAL_ID.PROVIDER),
                                      literalJsonEntry("created", EXTERNAL_ID.CREATED),
                                      literalJsonEntry("modified", EXTERNAL_ID.MODIFIED),
                                      literalJsonEntry("requested", EXTERNAL_ID.REQUESTED),
                                      literalJsonEntry(
                                          "versions", createQuery(VersionPolicy.ALL, null)))))
                          .from(
                              EXTERNAL_ID
                                  .join(ANALYSIS_EXTERNAL_ID)
                                  .on(EXTERNAL_ID.ID.eq(ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID)))
                          .where(ANALYSIS_EXTERNAL_ID.ANALYSIS_ID.eq(ANALYSIS.ID))),
                  literalJsonEntry(
                      "run",
                      DSL.field(
                          DSL.select(WORKFLOW_RUN.HASH_ID)
                              .from(WORKFLOW_RUN)
                              .where(WORKFLOW_RUN.ID.eq(ANALYSIS.WORKFLOW_RUN_ID))))))
          .from(ANALYSIS)
          .where(
              ANALYSIS
                  .HASH_ID
                  .eq(param("vidarrId", vidarrId))
                  .and(ANALYSIS.ANALYSIS_TYPE.eq(param("type", type))))
          .fetchOptional()
          .map(Record1::value1)
          .ifPresentOrElse(
              result -> {
                okJsonResponse(exchange, result.data());
              },
              () -> {
                notFoundResponse(exchange);
              });
    } catch (SQLException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void fetchFile(HttpServerExchange exchange) {
    fetchAnalysis(exchange, "file");
  }

  private void fetchMaxInFlight(HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
    exchange.setStatusCode(StatusCodes.OK);
    final OffsetDateTime endTime = OffsetDateTime.now();
    try (final JsonGenerator output = MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
      InFlightCountsByWorkflow counts = maxInFlightPerWorkflow.getCountsByWorkflow();
      output.writeStartObject();
      output.writeNumberField("timestamp", endTime.toInstant().toEpochMilli());
      output.writeObjectFieldStart("workflows");
      for (String workflow : counts.getWorkflows()) {
        output.writeObjectFieldStart(workflow);
        output.writeNumberField("currentInFlight", counts.getCurrent(workflow));
        output.writeNumberField("maxInFlight", counts.getMax(workflow));
        output.writeEndObject();
      }
      output.writeEndObject();
      output.writeEndObject();
    } catch (Exception e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void fetchProvenance(HttpServerExchange exchange, AnalysisProvenanceRequest request) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
    exchange.setStatusCode(StatusCodes.OK);
    final OffsetDateTime endTime = OffsetDateTime.now();
    epochLock.readLock().lock();
    try (final JsonGenerator output = MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
      if (request.getEpoch() != epoch) {
        request.setTimestamp(0);
      }
      output.writeStartObject();
      output.writeNumberField("epoch", epoch);
      output.writeNumberField("timestamp", endTime.toInstant().toEpochMilli());
      output.writeArrayFieldStart("results");
      try (final Connection connection = dataSource.getConnection()) {
        createAnalysisRecords(
            DSL.using(connection, SQLDialect.POSTGRES),
            output,
            request.getVersionPolicy(),
            request.getVersionTypes(),
            request.isIncludeParameters(),
            request.getAnalysisTypes(),
            WORKFLOW_RUN
                .MODIFIED
                .gt(Instant.ofEpochMilli(request.getTimestamp()).atOffset(ZoneOffset.UTC))
                .and(WORKFLOW_RUN.MODIFIED.le(endTime))
                .and(WORKFLOW_RUN.COMPLETED.isNotNull()));
        output.writeEndArray();
        output.writeEndObject();
      }
    } catch (IOException | SQLException e) {
      PROVENANCE_ERROR_COUNT.inc();
      internalServerErrorResponse(exchange, e);
    } finally {
      epochLock.readLock().unlock();
    }
  }

  private void fetchRecoveryFailures(HttpServerExchange exchange) {
    final Set<String> failureIds = processor.recoveryFailures();
    ArrayNode failureIdsResult = MAPPER.createArrayNode();
    failureIds.forEach(failureIdsResult::add);
    try {
      okJsonResponse(exchange, MAPPER.writeValueAsString(failureIdsResult));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void fetchRun(HttpServerExchange exchange) {
    try (final Connection connection = dataSource.getConnection()) {
      final String vidarrId =
          exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("hash");
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
      DSL.using(connection, SQLDialect.POSTGRES)
          .select(DSL.field(ACTIVE_WORKFLOW_RUN.ID.isNull()))
          .from(
              WORKFLOW_RUN
                  .leftJoin(ACTIVE_WORKFLOW_RUN)
                  .on(WORKFLOW_RUN.ID.eq(ACTIVE_WORKFLOW_RUN.ID)))
          .where(WORKFLOW_RUN.HASH_ID.eq(param("vidarrId", vidarrId)))
          .fetchOptional(Record1::value1)
          .ifPresentOrElse(
              complete -> {
                exchange.setStatusCode(StatusCodes.OK);
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
                try (final JsonGenerator output =
                    MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
                  createAnalysisRecords(
                      DSL.using(connection, SQLDialect.POSTGRES),
                      output,
                      VersionPolicy.ALL,
                      null,
                      true,
                      Set.of(AnalysisOutputType.FILE, AnalysisOutputType.URL),
                      WORKFLOW_RUN.HASH_ID.eq(param("vidarrId", vidarrId)));
                } catch (IOException | SQLException e) {
                  e.printStackTrace();
                }
              },
              () -> {
                notFoundResponse(exchange);
              });
    } catch (SQLException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void fetchStatus(HttpServerExchange exchange) {
    try (final Connection connection = dataSource.getConnection()) {
      final String vidarrId =
          exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("hash");
      processor.updateLastAccessed(vidarrId);
      DSL.using(connection, SQLDialect.POSTGRES)
          .select(DSL.jsonObject(STATUS_FIELDS))
          .from(
              WORKFLOW_RUN
                  .leftJoin(ACTIVE_WORKFLOW_RUN)
                  .on(WORKFLOW_RUN.ID.eq(ACTIVE_WORKFLOW_RUN.ID)))
          .where(WORKFLOW_RUN.HASH_ID.eq(param("vidarrId", vidarrId)))
          .fetchOptional(Record1::value1)
          .ifPresentOrElse(
              record -> {
                okJsonResponse(exchange, record.data());
              },
              () -> {
                notFoundResponse(exchange);
              });
    } catch (SQLException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void fetchTargets(HttpServerExchange exchange) {
    final ObjectNode targetsOutput = MAPPER.createObjectNode();
    for (final Entry<String, Target> target : targets.entrySet()) {
      final ObjectNode targetOutput = targetsOutput.putObject(target.getKey());
      final ArrayNode languages = targetOutput.putArray("language");
      for (final WorkflowLanguage language : WorkflowLanguage.values()) {
        if (target.getValue().engine().supports(language)) {
          languages.add(language.name());
        }
      }
      targetOutput.putPOJO(
          "engineParameters", target.getValue().engine().engineParameters().orElse(null));
      final ObjectNode inputProvisioners = targetOutput.putObject("inputProvisioners");
      for (final InputProvisionFormat inputFormat : InputProvisionFormat.values()) {
        final InputProvisioner<?> provisioner = target.getValue().provisionerFor(inputFormat);
        if (provisioner != null) {
          inputProvisioners.putPOJO(inputFormat.name(), provisioner.externalTypeFor(inputFormat));
        }
      }
      final ObjectNode outputProvisioners = targetOutput.putObject("outputProvisioners");
      for (final OutputProvisionFormat outputFormat : OutputProvisionFormat.values()) {
        final OutputProvisioner<?, ?> provisioner = target.getValue().provisionerFor(outputFormat);
        if (provisioner != null) {
          outputProvisioners.putPOJO(outputFormat.name(), provisioner.typeFor(outputFormat));
        }
      }
      final ObjectNode consumableResources = targetOutput.putObject("consumableResources");
      target
          .getValue()
          .consumableResources()
          .flatMap(cr -> cr.second().inputFromSubmitter().stream())
          .forEach(cr -> consumableResources.putPOJO(cr.first(), cr.second()));
    }
    try {
      okJsonResponse(exchange, MAPPER.writeValueAsString(targetsOutput));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void fetchUrl(HttpServerExchange exchange) {
    fetchAnalysis(exchange, "url");
  }

  private void fetchWorkflows(HttpServerExchange exchange) {
    try (final Connection connection = dataSource.getConnection()) {
      final WorkflowVersion workflowVersionAlias = WORKFLOW_VERSION.as("other_workflow_version");
      final JSON response =
          DSL.using(connection, SQLDialect.POSTGRES)
              .select(
                  DSL.jsonArrayAgg(
                      DSL.jsonObject(
                          literalJsonEntry("name", WORKFLOW_VERSION.NAME),
                          literalJsonEntry("version", WORKFLOW_VERSION.VERSION),
                          literalJsonEntry("metadata", WORKFLOW_VERSION.METADATA),
                          literalJsonEntry("parameters", WORKFLOW_VERSION.PARAMETERS),
                          literalJsonEntry(
                              "labels",
                              DSL.field(
                                  DSL.select(WORKFLOW.LABELS)
                                      .from(WORKFLOW)
                                      .where(WORKFLOW.NAME.eq(WORKFLOW_VERSION.NAME)))),
                          literalJsonEntry("language", WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE))))
              .from(
                  WORKFLOW_VERSION
                      .join(WORKFLOW_DEFINITION)
                      .on(WORKFLOW_VERSION.WORKFLOW_DEFINITION.eq(WORKFLOW_DEFINITION.ID)))
              .where(
                  WORKFLOW_VERSION.NAME.in(
                      DSL.select(WORKFLOW.NAME).from(WORKFLOW).where(WORKFLOW.IS_ACTIVE)))
              .fetchOptional()
              .orElseThrow()
              .value1();
      okJsonResponse(exchange, response == null ? "[]" : response.data());
    } catch (SQLException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void fetchWorkflow(HttpServerExchange exchange) {
    final String name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");

    try (final Connection connection = dataSource.getConnection()) {
      final Optional<String> result =
          DSL.using(connection, SQLDialect.POSTGRES)
              .select(
                  DSL.jsonObject(
                      literalJsonEntry("labels", WORKFLOW.LABELS),
                      literalJsonEntry("isActive", WORKFLOW.IS_ACTIVE),
                      literalJsonEntry("maxInFlight", WORKFLOW.MAX_IN_FLIGHT)))
              .from(WORKFLOW)
              .where(WORKFLOW.NAME.eq(name))
              .fetchOptional()
              .map(r -> r.value1().data());
      if (result.isPresent()) {
        okJsonResponse(exchange, result.get());
      } else {
        notFoundResponse(exchange);
      }
    } catch (SQLException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private JSONObjectNullStep<JSON> workflowVersionFields() {
    return DSL.jsonObject(
        literalJsonEntry("name", WORKFLOW_VERSION.NAME),
        literalJsonEntry("version", WORKFLOW_VERSION.VERSION),
        literalJsonEntry("id", WORKFLOW_VERSION.HASH_ID),
        literalJsonEntry("metadata", WORKFLOW_VERSION.METADATA),
        literalJsonEntry("parameters", WORKFLOW_VERSION.PARAMETERS),
        literalJsonEntry(
            "labels",
            DSL.field(
                DSL.select(WORKFLOW.LABELS)
                    .from(WORKFLOW)
                    .where(WORKFLOW.NAME.eq(WORKFLOW_VERSION.NAME)))),
        literalJsonEntry("language", WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE),
        literalJsonEntry("outputs", WORKFLOW_VERSION.METADATA));
  }

  private JSONObjectNullStep<JSON> workflowVersionWithDefinitionFields() {
    final WorkflowDefinition accessoryDefinition =
        WORKFLOW_DEFINITION.as("accessoryWorkflowDefinition");
    return DSL.jsonObject(
        literalJsonEntry("name", WORKFLOW_VERSION.NAME),
        literalJsonEntry("version", WORKFLOW_VERSION.VERSION),
        literalJsonEntry("id", WORKFLOW_VERSION.HASH_ID),
        literalJsonEntry("metadata", WORKFLOW_VERSION.METADATA),
        literalJsonEntry("parameters", WORKFLOW_VERSION.PARAMETERS),
        literalJsonEntry(
            "labels",
            DSL.field(
                DSL.select(WORKFLOW.LABELS)
                    .from(WORKFLOW)
                    .where(WORKFLOW.NAME.eq(WORKFLOW_VERSION.NAME)))),
        literalJsonEntry("language", WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE),
        literalJsonEntry("outputs", WORKFLOW_VERSION.METADATA),
        literalJsonEntry("workflow", WORKFLOW_DEFINITION.WORKFLOW_FILE),
        literalJsonEntry(
            "accessoryFiles",
            DSL.coalesce(
                DSL.field(
                    DSL.select(
                            DSL.jsonObjectAgg(
                                    WORKFLOW_VERSION_ACCESSORY.FILENAME,
                                    accessoryDefinition.WORKFLOW_FILE)
                                .absentOnNull())
                        .from(
                            WORKFLOW_VERSION_ACCESSORY
                                .join(accessoryDefinition)
                                .on(
                                    accessoryDefinition.ID.eq(
                                        WORKFLOW_VERSION_ACCESSORY.WORKFLOW_DEFINITION))
                                .where(
                                    WORKFLOW_VERSION_ACCESSORY.WORKFLOW_VERSION.eq(
                                        WORKFLOW_VERSION.ID)))),
                DSL.inline(JSON.json("{}")))));
  }

  private void fetchWorkflowVersion(HttpServerExchange exchange) {
    final String name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");
    final String version =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("version");
    final Boolean includeWorkflowDefinitions =
        Arrays.stream(exchange.getQueryString().split("\\&"))
            .map(q -> q.split("="))
            .filter(q -> "includeDefinitions".equals(q[0]))
            .map(q -> q[1])
            .map(Boolean::parseBoolean)
            .findAny()
            .orElse(false);
    try (final Connection connection = dataSource.getConnection()) {
      JSONObjectNullStep<JSON> fields =
          (includeWorkflowDefinitions
              ? workflowVersionWithDefinitionFields()
              : workflowVersionFields());
      final Optional<String> result =
          DSL
              .using(connection, SQLDialect.POSTGRES)
              .select(fields)
              .from(
                  WORKFLOW_VERSION
                      .join(WORKFLOW_DEFINITION)
                      .on(WORKFLOW_VERSION.WORKFLOW_DEFINITION.eq(WORKFLOW_DEFINITION.ID)))
              .where(
                  WORKFLOW_VERSION
                      .NAME
                      .eq(param("workflowName", name))
                      .and(WORKFLOW_VERSION.VERSION.eq(param("workflowVersion", version))))
              .fetchOptional()
              .stream()
              .map(
                  r -> {
                    if (r.value1() != null) {
                      return r.value1().data();
                    }
                    return null;
                  })
              .filter(Objects::nonNull)
              .findFirst();
      if (result.isPresent()) {
        okJsonResponse(exchange, result.get());
      } else {
        notFoundResponse(exchange);
      }
    } catch (SQLException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  @Override
  public Stream<Header> headers() {
    return Stream.empty();
  }

  private void insertAnalysis(
      Configuration configuration,
      long id,
      ProvenanceAnalysisRecord<ca.on.oicr.gsi.vidarr.api.ExternalId> analysis) {
    final Integer analysisId =
        DSL.using(configuration)
            .insertInto(ANALYSIS)
            .set(ANALYSIS.WORKFLOW_RUN_ID, param("workflowRunId", id))
            .set(ANALYSIS.HASH_ID, param("hashId", analysis.getId()))
            .set(ANALYSIS.ANALYSIS_TYPE, param("analysisType", analysis.getType()))
            .set(ANALYSIS.CREATED, param("created", analysis.getCreated().toOffsetDateTime()))
            .set(
                ANALYSIS.FILE_PATH,
                param(
                    "filePath",
                    analysis.getType().equals("file") ? analysis.getPath() : analysis.getUrl()))
            .set(
                ANALYSIS.FILE_CHECKSUM,
                param(
                    "checksum", analysis.getType().equals("file") ? analysis.getChecksum() : null))
            .set(
                ANALYSIS.FILE_CHECKSUM_TYPE,
                param(
                    "checksumType",
                    analysis.getType().equals("file") ? analysis.getChecksumType() : null))
            .set(
                ANALYSIS.FILE_METATYPE,
                param(
                    "fileMetatype",
                    analysis.getType().equals("file") ? analysis.getMetatype() : null))
            .set(ANALYSIS.FILE_SIZE, analysis.getType().equals("file") ? analysis.getSize() : null)
            .set(
                ANALYSIS.LABELS,
                param("labels", DatabaseWorkflow.labelsToJson(analysis.getLabels())))
            .returningResult(ANALYSIS.ID)
            .fetchOptional()
            .orElseThrow()
            .value1();
    InsertValuesStep2<AnalysisExternalIdRecord, Integer, Integer> associate =
        DSL.using(configuration)
            .insertInto(ANALYSIS_EXTERNAL_ID)
            .columns(ANALYSIS_EXTERNAL_ID.ANALYSIS_ID, ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID);
    for (final ExternalId externalId : analysis.getExternalKeys()) {
      associate =
          associate.values(
              DSL.val(analysisId),
              DSL.field(
                  DSL.select(EXTERNAL_ID.ID)
                      .from(EXTERNAL_ID)
                      .where(
                          EXTERNAL_ID
                              .WORKFLOW_RUN_ID
                              .eq(param("workflowRunId", id))
                              .and(
                                  EXTERNAL_ID.PROVIDER.eq(
                                      param("externalIdProvider", externalId.getProvider())))
                              .and(
                                  EXTERNAL_ID.EXTERNAL_ID_.eq(
                                      param("externalId", externalId.getId()))))));
    }
    associate.execute();
  }

  private void insertExternalKey(
      Configuration configuration, long id, ExternalMultiVersionKey externalId) {
    final Integer externalIdDbId =
        DSL.using(configuration)
            .insertInto(EXTERNAL_ID)
            .set(EXTERNAL_ID.PROVIDER, param("provider", externalId.getProvider()))
            .set(EXTERNAL_ID.EXTERNAL_ID_, param("externalId", externalId.getId()))
            .set(EXTERNAL_ID.WORKFLOW_RUN_ID, param("workflowRunId", id))
            .returningResult(EXTERNAL_ID.ID)
            .fetchOptional()
            .orElseThrow()
            .value1();
    InsertValuesStep3<ExternalIdVersionRecord, Integer, String, String> insertVersions =
        DSL.using(configuration)
            .insertInto(EXTERNAL_ID_VERSION)
            .columns(
                EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                EXTERNAL_ID_VERSION.KEY,
                EXTERNAL_ID_VERSION.VALUE);
    for (final Entry<String, Set<String>> version : externalId.getVersions().entrySet()) {
      for (final String value : version.getValue()) {
        insertVersions = insertVersions.values(externalIdDbId, version.getKey(), value);
      }
    }
    insertVersions.execute();
  }

  private void insertWorkflowDefinition(
      Configuration configuration,
      String workflowScript,
      String rootWorkflowHash,
      WorkflowLanguage workflowLanguage) {
    DSL.using(configuration)
        .insertInto(WORKFLOW_DEFINITION)
        .set(WORKFLOW_DEFINITION.WORKFLOW_FILE, param("workflowFile", workflowScript))
        .set(WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE, param("workflowLanguage", workflowLanguage))
        .set(WORKFLOW_DEFINITION.HASH_ID, param("hashId", rootWorkflowHash))
        .onConflict(WORKFLOW_DEFINITION.HASH_ID)
        .doNothing()
        .execute();
  }

  private Optional<Long> insertWorkflowRun(
      Configuration configuration,
      int workflowId,
      OffsetDateTime now,
      ca.on.oicr.gsi.vidarr.api.ProvenanceWorkflowRun<ExternalMultiVersionKey> run) {
    return DSL.using(configuration)
        .insertInto(WORKFLOW_RUN)
        .set(WORKFLOW_RUN.HASH_ID, param("hashId", run.getId()))
        .set(WORKFLOW_RUN.WORKFLOW_VERSION_ID, param("workflowVersionId", workflowId))
        .set(
            WORKFLOW_RUN.ENGINE_PARAMETERS,
            run.getEngineParameters()) // JsonNode and ObjectNode are not accepted by param
        .set(WORKFLOW_RUN.ARGUMENTS, run.getArguments())
        .set(WORKFLOW_RUN.METADATA, run.getMetadata())
        .set(WORKFLOW_RUN.LABELS, param("labels", DatabaseWorkflow.labelsToJson(run.getLabels())))
        .set(
            WORKFLOW_RUN.INPUT_FILE_IDS,
            param("inputFileIds", run.getInputFiles().toArray(String[]::new)))
        .set(WORKFLOW_RUN.CREATED, param("created", run.getCreated().toOffsetDateTime()))
        .set(WORKFLOW_RUN.COMPLETED, param("completed", run.getCompleted().toOffsetDateTime()))
        .set(WORKFLOW_RUN.LAST_ACCESSED, param("lastAccessed", now))
        .set(
            WORKFLOW_RUN.STARTED,
            param("started", run.getStarted() == null ? null : run.getStarted().toOffsetDateTime()))
        .onConflict(WORKFLOW_RUN.HASH_ID)
        .doNothing()
        .returningResult(WORKFLOW_RUN.ID)
        .fetchOptional()
        .map(Record1::value1);
  }

  private Optional<Record1<Integer>> insertWorkflowVersion(
      Configuration configuration,
      String workflowName,
      String rootWorkflowHash,
      String workflowVersionHashId,
      String workflowVersion,
      Map<String, OutputType> outputs,
      Map<String, InputType> parameters) {
    return DSL.using(configuration)
        .insertInto(WORKFLOW_VERSION)
        .set(WORKFLOW_VERSION.HASH_ID, param("hashId", workflowVersionHashId))
        .set(WORKFLOW_VERSION.NAME, param("name", workflowName))
        .set(WORKFLOW_VERSION.VERSION, param("version", workflowVersion))
        .set(
            WORKFLOW_VERSION.METADATA,
            MAPPER.<ObjectNode>valueToTree(
                outputs)) // ObjectNode is not supported type in POSTGRES, param can't be bound
        .set(WORKFLOW_VERSION.PARAMETERS, MAPPER.<ObjectNode>valueToTree(parameters))
        .set(
            WORKFLOW_VERSION.WORKFLOW_DEFINITION,
            DSL.select(WORKFLOW_DEFINITION.ID)
                .from(WORKFLOW_DEFINITION)
                .where(WORKFLOW_DEFINITION.HASH_ID.eq(param("rootWorkflowHash", rootWorkflowHash))))
        .onConflict(WORKFLOW_VERSION.NAME, WORKFLOW_VERSION.VERSION)
        .doNothing()
        .returningResult(WORKFLOW_VERSION.ID)
        .fetchOptional();
  }

  private void load(HttpServerExchange exchange, UnloadedData unloadedData, boolean verify) {
    try {
      // We have to hold a very expensive lock to load data in the database, so we're going to do an
      // offline validation of the data to make sure it's self-consistent, then acquire the lock and
      // do an online validation.

      // Map of Workflow Name to Pair of UnloadedWorkflow and Map of Workflow Version to Pair of
      // workflow version hash and UnloadedWorkflowVersion
      final TreeMap<
              String, Pair<UnloadedWorkflow, Map<String, Pair<String, UnloadedWorkflowVersion>>>>
          workflowInfo = new TreeMap<>();

      // First, validate the workflows
      // Data we would like to load must not have duplicate workflows
      // If not a duplicate, track the workflow as known
      for (final UnloadedWorkflow workflow : unloadedData.getWorkflows()) {
        if (workflowInfo.containsKey(workflow.getName())) {
          badRequestResponse(
              exchange,
              String.format("Duplicate workflow %s in load request.", workflow.getName()));
          return;
        }
        workflowInfo.put(workflow.getName(), new Pair<>(workflow, new TreeMap<>()));
      }

      // Data we would like to load must reference valid workflow versions
      // 1. Can't have a version for an unknown workflow
      for (final UnloadedWorkflowVersion workflowVersion : unloadedData.getWorkflowVersions()) {
        final Pair<UnloadedWorkflow, Map<String, Pair<String, UnloadedWorkflowVersion>>> info =
            workflowInfo.get(workflowVersion.getName());
        if (info == null) {
          badRequestResponse(
              exchange,
              String.format(
                  "Workflow version references unknown workflow %s in load request.",
                  workflowVersion.getName()));
          return;
        }

        // 2. Can't have a duplicate workflow version
        if (info.second().containsKey(workflowVersion.getVersion())) {
          badRequestResponse(
              exchange,
              String.format(
                  "Duplicate workflow version %s/%s in load request.",
                  workflowVersion.getName(), workflowVersion.getVersion()));
          return;
        }

        // Success; compute hash ID and store in map
        final String definitionHash = generateWorkflowDefinitionHash(workflowVersion.getWorkflow());
        final TreeMap<String, String> accessoryHashes =
            generateAccessoryWorkflowHashes(workflowVersion.getAccessoryFiles());
        final String workflowVersionHash =
            generateWorkflowVersionHash(
                workflowVersion.getName(),
                workflowVersion.getVersion(),
                definitionHash,
                workflowVersion.getOutputs(),
                workflowVersion.getParameters(),
                accessoryHashes);

        info.second()
            .put(workflowVersion.getVersion(), new Pair<>(workflowVersionHash, workflowVersion));
      }

      // Validate the workflow runs; all workflow data must be included, so we don't need the DB for
      // this.
      final TreeSet<String> seenWorkflowRunIds = new TreeSet<>();
      for (final ProvenanceWorkflowRun<ExternalMultiVersionKey> workflowRun :
          unloadedData.getWorkflowRuns()) {

        // All workflow runs must be of installed workflows
        final Pair<UnloadedWorkflow, Map<String, Pair<String, UnloadedWorkflowVersion>>> info =
            workflowInfo.get(workflowRun.getWorkflowName());
        if (info == null) {
          badRequestResponse(
              exchange,
              String.format(
                  "Workflow run %s references missing workflow %s in load request.",
                  workflowRun.getId(), workflowRun.getWorkflowName()));
          return;
        }

        // All workflow runs must be of installed workflow versions
        final Pair<String, UnloadedWorkflowVersion> versionInfo =
            info.second().get(workflowRun.getWorkflowVersion());
        if (versionInfo == null) {
          badRequestResponse(
              exchange,
              String.format(
                  "Workflow run %s references missing workflow version %s/%s in load request.",
                  workflowRun.getId(),
                  workflowRun.getWorkflowName(),
                  workflowRun.getWorkflowVersion()));
          return;
        }

        // Validate that the labels are what we expect and can resolve to Vidarr types
        final List<String> labelErrors =
            DatabaseBackedProcessor.validateLabels(
                    workflowRun.getLabels(), info.first().getLabels())
                .collect(Collectors.toList());
        if (!labelErrors.isEmpty()) {
          badRequestResponse(
              exchange,
              String.format(
                  "Workflow run %s has bad labels: %s",
                  workflowRun.getId(), String.join("; ", labelErrors)));
          return;
        }

        // Check that the External Keys do not contain duplicates
        final Set<Pair<String, String>> knownExternalIds =
            workflowRun.getExternalKeys().stream()
                .map(e -> new Pair<>(e.getProvider(), e.getId()))
                .collect(Collectors.toSet());
        if (knownExternalIds.size() != workflowRun.getExternalKeys().size()) {
          badRequestResponse(
              exchange,
              String.format("Workflow run %s has duplicate external keys", workflowRun.getId()));
          return;
        }

        // Compute the hash ID this workflow run should have and compare it to the one we received
        // if we want to verify the run, otherwise apply it directly.
        final String correctId =
            DatabaseBackedProcessor.computeWorkflowRunHashId(
                workflowRun.getWorkflowName(),
                workflowRun.getLabels(),
                info.first().getLabels() == null ? Set.of() : info.first().getLabels().keySet(),
                versionInfo.second().getParameters().entrySet().stream()
                    .flatMap(
                        param ->
                            param
                                .getValue()
                                .apply(
                                    new ExtractInputVidarrIds(
                                        MAPPER, workflowRun.getArguments().get(param.getKey()))))
                    .collect(Collectors.toCollection(TreeSet::new)),
                workflowRun.getExternalKeys());

        if (verify) {
          if (!correctId.equals(workflowRun.getId())) {
            badRequestResponse(
                exchange,
                String.format(
                    "Workflow run %s should have ID %s in load request.",
                    workflowRun.getId(), correctId));
            return;
          }
        } else {
          workflowRun.setId(correctId);
        }

        // Cannot have the same workflow run multiple times in the request
        if (!seenWorkflowRunIds.add(correctId)) {
          badRequestResponse(
              exchange,
              String.format(
                  "Workflow run %s is included multiple times in the input.", workflowRun.getId()));
          return;
        }

        // Workflow run must have some analysis
        if (workflowRun.getAnalysis() == null || workflowRun.getAnalysis().isEmpty()) {
          badRequestResponse(
              exchange, String.format("Workflow run %s has no analysis.", workflowRun.getId()));
          return;
        }

        for (final ProvenanceAnalysisRecord<ExternalId> output : workflowRun.getAnalysis()) {
          // Every analysis record must have at least one external key
          if (output.getExternalKeys().isEmpty()) {
            badRequestResponse(
                exchange,
                String.format(
                    "Workflow run %s has output %s that is not associated with any external"
                        + " identifiers.",
                    workflowRun.getId(), output.getId()));
            return;
          }

          // Compare the external IDs in each analysis record to the set of External IDs we built
          // when checking for duplicate external keys and ensure every external ID in the
          // analysis record corresponds to an external ID from the workflow run
          for (final ExternalId externalId : output.getExternalKeys()) {
            if (!knownExternalIds.contains(
                new Pair<>(externalId.getProvider(), externalId.getId()))) {
              badRequestResponse(
                  exchange,
                  String.format(
                      "Workflow run %s has output %s that references unknown external ID"
                          + " %s/%s.",
                      workflowRun.getId(),
                      output.getId(),
                      externalId.getProvider(),
                      externalId.getId()));
              return;
            }
          }

          // The only valid output types for analysis record are file and url
          if (!output.getType().equals("file") && !output.getType().equals("url")) {
            badRequestResponse(
                exchange,
                String.format(
                    "Workflow run %s has output %s that unknown output type %s.",
                    workflowRun.getId(), output.getId(), output.getType()));
            return;
          }

          // Validate all required metadata is present for file typed analysis record
          if (output.getType().equals("file")
              && (output.getMetatype() == null
                  || output.getMetatype().isBlank()
                  || output.getChecksum() == null
                  || output.getChecksum().isBlank()
                  || output.getChecksumType() == null
                  || output.getChecksumType().isBlank())) {
            badRequestResponse(
                exchange,
                String.format(
                    "Workflow run %s has file output %s with missing information.",
                    workflowRun.getId(), output.getId()));
            return;
          }

          // Calculate the hash of the analysis record and compare to the hash we received if
          // we want to verify, else apply it
          final MessageDigest fileDigest = MessageDigest.getInstance("SHA-256");
          fileDigest.update(workflowRun.getId().getBytes(StandardCharsets.UTF_8));
          fileDigest.update(
              (output.getType().equals("file")
                      ? Path.of(output.getPath()).getFileName().toString()
                      : output.getUrl())
                  .getBytes(StandardCharsets.UTF_8));
          final String correctOutputId = hexDigits(fileDigest.digest());

          if (verify) {
            if (!output.getId().equals(correctOutputId)) {
              badRequestResponse(
                  exchange,
                  String.format(
                      "Workflow run %s has analysis %s that should have ID %s.",
                      workflowRun.getId(), output.getId(), correctOutputId));
              return;
            }
          } else {
            output.setId(correctOutputId);
          }
        }
      }

      // Okay, if we made it this far, the file is theoretically loadable. There needs to be
      // additional validation against the database (loadDataIntoDatabase()), but we will do that in
      // a transaction with the expensive lock.
      if (!loadCounter.tryAcquire()) {
        exchange.setStatusCode(StatusCodes.INSUFFICIENT_STORAGE);
        exchange
            .getResponseSender()
            .send(
                "There are too many load/unload requests queued right now. Please try again"
                    + " later.");
        return;
      }
      epochLock.writeLock().lock();
      loadCounter.release();
      try (final Connection connection = dataSource.getConnection()) {
        DSL.using(connection, SQLDialect.POSTGRES)
            .transaction(
                configuration -> loadDataIntoDatabase(unloadedData, workflowInfo, configuration));
        okEmptyResponse(exchange);
      } catch (IllegalArgumentException e) {
        badRequestResponse(exchange, e.getMessage());
      } catch (Exception e) {
        internalServerErrorResponse(exchange, e);
      } finally {
        epochLock.writeLock().unlock();
      }
    } catch (JsonProcessingException | NoSuchAlgorithmException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private TreeMap<String, String> generateAccessoryWorkflowHashes(
      Map<String, String> accessoryFiles) throws NoSuchAlgorithmException {
    final TreeMap<String, String> accessoryHashes = new TreeMap<>();
    if (accessoryFiles == null) return accessoryHashes;
    for (final Entry<String, String> accessory : new TreeMap<>(accessoryFiles).entrySet()) {
      final String accessoryHash = generateWorkflowDefinitionHash(accessory.getValue());
      accessoryHashes.put(accessory.getKey(), accessoryHash);
    }
    return accessoryHashes;
  }

  /**
   * Perform additional validation and insert into database
   *
   * @param unloadedData
   * @param workflowInfo
   * @param configuration
   * @throws JsonProcessingException
   * @throws NoSuchAlgorithmException
   */
  private void loadDataIntoDatabase(
      UnloadedData unloadedData,
      TreeMap<String, Pair<UnloadedWorkflow, Map<String, Pair<String, UnloadedWorkflowVersion>>>>
          workflowInfo,
      Configuration configuration)
      throws JsonProcessingException, NoSuchAlgorithmException {
    // Map of Workflow Name to Workflow Version IDs (UnloadedWorkflowVersion.version to
    // id.get.value1??)
    final TreeMap<String, Map<String, Integer>> workflowId = new TreeMap<>();
    // Insert the workflow and workflow version from workflowInfo
    for (final Pair<UnloadedWorkflow, Map<String, Pair<String, UnloadedWorkflowVersion>>> info :
        workflowInfo.values()) {
      final String workflowName = info.first().getName();
      final Map<String, BasicType> workflowLabels = info.first().getLabels();
      Map<?, ?> labels =
          Objects.requireNonNullElse(
              upsertWorkflowReturningLabels(configuration, workflowName, workflowLabels), Map.of());
      if (!labels.equals(Objects.requireNonNullElse(workflowLabels, Map.of()))) {
        throw new IllegalArgumentException(
            String.format(
                "Workflow %s has mismatched labels to what is in the database."
                    + " Cannot load this data.",
                workflowName));
      }
      final TreeMap<String, Integer> workflowVersionIds = new TreeMap<>();
      workflowId.put(workflowName, workflowVersionIds);
      for (final Pair<String, UnloadedWorkflowVersion> version : info.second().values()) {
        final String workflowScript = version.second().getWorkflow();
        final String rootWorkflowHash = generateWorkflowDefinitionHash(workflowScript);

        WorkflowLanguage workflowLanguage = version.second().getLanguage();
        insertWorkflowDefinition(configuration, workflowScript, rootWorkflowHash, workflowLanguage);

        final String workflowVersionHashId = version.first();
        final String workflowVersion = version.second().getVersion();
        final Map<String, OutputType> outputs = version.second().getOutputs();
        final Map<String, InputType> parameters = version.second().getParameters();
        final Optional<Record1<Integer>> id =
            insertWorkflowVersion(
                configuration,
                workflowName,
                rootWorkflowHash,
                workflowVersionHashId,
                workflowVersion,
                outputs,
                parameters);
        // We will have no ID from the insert if it already there
        if (id.isPresent()) {
          workflowVersionIds.put(workflowVersion, id.get().value1());
          for (final Entry<String, String> accessory :
              version.second().getAccessoryFiles().entrySet()) {
            final String accessoryWorkflowHash =
                generateWorkflowDefinitionHash(accessory.getValue());
            insertWorkflowDefinition(
                configuration, accessory.getValue(), accessoryWorkflowHash, workflowLanguage);
            associateDefinitionAsAccessory(
                configuration, id.get().value1(), accessory.getKey(), accessoryWorkflowHash);
          }
        } else {
          workflowVersionIds.put(
              workflowVersion,
              DSL.using(configuration)
                  .select(WORKFLOW_VERSION.ID)
                  .from(WORKFLOW_VERSION)
                  .where(
                      WORKFLOW_VERSION.HASH_ID.eq(param("workflow-hash-id", workflowVersionHashId)))
                  .fetchOptional(WORKFLOW_VERSION.ID)
                  .orElseThrow());
        }
      }
    }
    final OffsetDateTime now = OffsetDateTime.now();
    for (final ProvenanceWorkflowRun<ExternalMultiVersionKey> run :
        unloadedData.getWorkflowRuns()) {
      final Optional<Long> id =
          insertWorkflowRun(
              configuration,
              workflowId.get(run.getWorkflowName()).get(run.getWorkflowVersion()),
              now,
              run);
      if (id.isPresent()) {
        for (final ExternalMultiVersionKey externalId : run.getExternalKeys()) {

          insertExternalKey(configuration, id.get(), externalId);
        }
        for (final ProvenanceAnalysisRecord<ExternalId> analysis : run.getAnalysis()) {
          insertAnalysis(configuration, id.get(), analysis);
        }
      }
    }
    epoch = Instant.now().toEpochMilli();
  }

  @Override
  public String name() {
    return "Víðarr";
  }

  @Override
  public Stream<NavigationMenu> navigation() {
    return Stream.empty();
  }

  private void recover() throws SQLException {
    final ArrayList<Runnable> recoveredWorkflows = new ArrayList<>();
    processor.recover(recoveredWorkflows::add, this.maxInFlightPerWorkflow, outputProvisioners);
    if (recoveredWorkflows.isEmpty()) {
      System.err.println("No unstarted workflows in the database. Resuming normal operation.");
    } else {
      System.err.printf(
          "Recovering %d unstarted workflows from the database.\n", recoveredWorkflows.size());
      recoveredWorkflows.forEach(Runnable::run);
    }
  }

  private void retryProvisionOut(HttpServerExchange exchange, RetryProvisionOutRequest request) {
    try {
      final List<String> ids = processor.retry(Optional.ofNullable(request.getWorkflowRunIds()));
      exchange.setStatusCode(StatusCodes.OK);
      exchange.getResponseSender().send(MAPPER.writeValueAsString(ids));
    } catch (Exception e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void status(HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html; charset=utf8");
    exchange.setStatusCode(StatusCodes.OK);
    status.renderPage(exchange.getOutputStream());
  }

  private void submit(HttpServerExchange exchange, SubmitWorkflowRequest body) {
    final AtomicReference<Runnable> postCommitAction = new AtomicReference<>();
    final Pair<Integer, SubmitWorkflowResponse> response;
    if (!canSubmit) {
      response =
          new Pair<>(
              400, new SubmitWorkflowResponseFailure("This Vidarr does not allow submissions"));
    } else {
      response =
          processor.submit(
              body.getTarget(),
              body.getWorkflow(),
              body.getWorkflowVersion(),
              body.getLabels(),
              body.getArguments(),
              body.getEngineParameters(),
              body.getMetadata(),
              body.getExternalKeys(),
              body.getConsumableResources(),
              body.getAttempt(),
              this.maxInFlightPerWorkflow,
              new DatabaseBackedProcessor.SubmissionResultHandler<>() {
                @Override
                public boolean allowLaunch() {
                  return body.getMode() == SubmitMode.RUN;
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> dryRunResult() {
                  return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseDryRun());
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> externalIdMismatch(String error) {
                  return new Pair<>(
                      StatusCodes.BAD_REQUEST,
                      new SubmitWorkflowResponseFailure("External IDs do not match: " + error));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> internalError(Exception e) {
                  e.printStackTrace();
                  return new Pair<>(
                      StatusCodes.INTERNAL_SERVER_ERROR,
                      new SubmitWorkflowResponseFailure(e.getMessage()));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> invalidWorkflow(Set<String> errors) {
                  return new Pair<>(
                      StatusCodes.BAD_REQUEST, new SubmitWorkflowResponseFailure(errors));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> launched(
                    String vidarrId, Runnable start) {
                  postCommitAction.set(start);
                  return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseSuccess(vidarrId));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> matchExisting(String vidarrId) {
                  return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseSuccess(vidarrId));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> missingExternalIdVersion() {
                  return new Pair<>(
                      StatusCodes.BAD_REQUEST,
                      new SubmitWorkflowResponseFailure("External IDs do not have versions set."));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> missingExternalKeyVersions(
                    String vidarrId, List<ExternalKey> missingKeys) {
                  return new Pair<>(
                      StatusCodes.BAD_REQUEST,
                      new SubmitWorkflowResponseMissingKeyVersions(vidarrId, missingKeys));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> multipleMatches(
                    List<String> matchIds) {
                  return new Pair<>(
                      StatusCodes.CONFLICT, new SubmitWorkflowResponseConflict(matchIds));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> reinitialise(
                    String vidarrId, Runnable start) {
                  postCommitAction.set(start);
                  return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseSuccess(vidarrId));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> unknownTarget(String targetName) {
                  return new Pair<>(
                      StatusCodes.BAD_REQUEST,
                      new SubmitWorkflowResponseFailure(
                          String.format("Target %s is unknown", targetName)));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> unknownWorkflow(
                    String name, String version) {
                  return new Pair<>(
                      StatusCodes.BAD_REQUEST,
                      new SubmitWorkflowResponseFailure(
                          String.format("Workflow %s (%s) is unknown", name, version)));
                }

                @Override
                public Pair<Integer, SubmitWorkflowResponse> unresolvedIds(
                    TreeSet<String> inputId) {
                  return new Pair<>(
                      StatusCodes.BAD_REQUEST,
                      new SubmitWorkflowResponseFailure(
                          inputId.stream()
                              .map(id -> String.format("Input ID %s cannot be resolved", id))
                              .collect(Collectors.toList())));
                }
              });
    }
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
    exchange.setStatusCode(response.first());
    if (postCommitAction.get() != null) {
      postCommitAction.get().run();
    }
    try {
      exchange.getResponseSender().send(MAPPER.writeValueAsString(response.second()));
    } catch (JsonProcessingException e) {
      internalServerErrorResponse(exchange, e);
    }
  }

  private void unload(HttpServerExchange exchange, UnloadRequest request) {
    if (!loadCounter.tryAcquire()) {
      exchange.setStatusCode(StatusCodes.INSUFFICIENT_STORAGE);
      exchange
          .getResponseSender()
          .send(
              "There are too many load/unload requests queued right now. Please try again later.");
      return;
    }
    epochLock.writeLock().lock();
    loadCounter.release();
    try {
      // Non-recursive unload is not allowed.
      request.setRecursive(true);
      final UnloadResponse response =
          unloadSearch(
              request,
              (tx, ids) -> {
                final Instant time = Instant.now();
                final String filename = String.format("unload-%s.json", time);
                try (final JsonGenerator output =
                    MAPPER_FACTORY.createGenerator(
                        Files.newOutputStream(unloadDirectory.resolve(filename)))) {
                  dumpUnloadDataToJson(tx, ids, output);
                }

                tx.dsl()
                    .delete(ANALYSIS_EXTERNAL_ID)
                    .where(
                        ANALYSIS_EXTERNAL_ID.ANALYSIS_ID.in(
                            DSL.select(ANALYSIS.ID)
                                .from(ANALYSIS)
                                .where(ANALYSIS.WORKFLOW_RUN_ID.eq(DSL.any(ids)))))
                    .execute();
                tx.dsl()
                    .delete(ANALYSIS)
                    .where(ANALYSIS.WORKFLOW_RUN_ID.eq(DSL.any(ids)))
                    .execute();
                tx.dsl()
                    .delete(EXTERNAL_ID_VERSION)
                    .where(
                        EXTERNAL_ID_VERSION.EXTERNAL_ID_ID.in(
                            DSL.select(EXTERNAL_ID.ID)
                                .from(EXTERNAL_ID)
                                .where(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(DSL.any(ids)))))
                    .execute();
                tx.dsl()
                    .delete(EXTERNAL_ID)
                    .where(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(DSL.any(ids)))
                    .execute();
                List<String> hashes =
                    tx.dsl()
                        .delete(WORKFLOW_RUN)
                        .where(WORKFLOW_RUN.ID.eq(DSL.any(ids)))
                        .returningResult(DSL.field(WORKFLOW_RUN.HASH_ID))
                        .fetch()
                        .map(h -> h.value1());
                UnloadResponse res = new UnloadResponse();
                res.setFilename(filename);
                res.setDeletedWorkflowRuns(hashes);
                epoch = time.toEpochMilli();
                return res;
              });
      okJsonResponse(exchange, MAPPER.writeValueAsString(response));
    } catch (Exception e) {
      internalServerErrorResponse(exchange, e);
    } finally {
      epochLock.writeLock().unlock();
    }
  }

  private <T> T unloadSearch(UnloadRequest request, UnloadProcessor<T> handleWorkflowRuns)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      return DSL.using(connection, SQLDialect.POSTGRES)
          .transactionResult(
              configuration -> {
                final TreeSet<Long> workflowRuns =
                    new TreeSet<>(
                        DSL.using(configuration)
                            .select(WORKFLOW_RUN.ID)
                            .from(
                                WORKFLOW_RUN
                                    .join(WORKFLOW_VERSION)
                                    .on(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID)))
                            .where(
                                WORKFLOW_RUN
                                    .COMPLETED
                                    .isNotNull()
                                    .and(
                                        request
                                            .getFilter()
                                            .convert(
                                                new UnloadFilter.Visitor<Condition>() {
                                                  @Override
                                                  public Condition analysisId(Stream<String> ids) {
                                                    return DSL.exists(
                                                        DSL.select()
                                                            .from(ANALYSIS)
                                                            .where(
                                                                WORKFLOW_RUN
                                                                    .ID
                                                                    .eq(ANALYSIS.WORKFLOW_RUN_ID)
                                                                    .and(
                                                                        match(
                                                                            ANALYSIS.HASH_ID,
                                                                            ids))));
                                                  }

                                                  @Override
                                                  public Condition and(Stream<Condition> clauses) {
                                                    return clauses
                                                        .reduce(Condition::and)
                                                        .orElseGet(() -> DSL.condition(false));
                                                  }

                                                  @Override
                                                  public Condition completedAfter(Instant time) {
                                                    return WORKFLOW_RUN.COMPLETED.ge(
                                                        time.atOffset(ZoneOffset.UTC));
                                                  }

                                                  @Override
                                                  public Condition externalKey(
                                                      Stream<String> providers) {
                                                    return DSL.exists(
                                                        DSL.select()
                                                            .from(EXTERNAL_ID)
                                                            .where(
                                                                WORKFLOW_RUN
                                                                    .ID
                                                                    .eq(EXTERNAL_ID.WORKFLOW_RUN_ID)
                                                                    .and(
                                                                        match(
                                                                            EXTERNAL_ID.PROVIDER,
                                                                            providers))));
                                                  }

                                                  @Override
                                                  public Condition externalKey(
                                                      String provider, Stream<String> ids) {
                                                    return DSL.exists(
                                                        DSL.select()
                                                            .from(EXTERNAL_ID)
                                                            .where(
                                                                WORKFLOW_RUN
                                                                    .ID
                                                                    .eq(EXTERNAL_ID.WORKFLOW_RUN_ID)
                                                                    .and(
                                                                        EXTERNAL_ID.PROVIDER.eq(
                                                                            provider))
                                                                    .and(
                                                                        match(
                                                                            EXTERNAL_ID
                                                                                .EXTERNAL_ID_,
                                                                            ids))));
                                                  }

                                                  @Override
                                                  public Condition lastSubmittedAfter(
                                                      Instant time) {
                                                    return WORKFLOW_RUN.LAST_ACCESSED.ge(
                                                        time.atOffset(ZoneOffset.UTC));
                                                  }

                                                  private Condition match(
                                                      Field<String> field, Stream<String> values) {
                                                    final Set<String> items =
                                                        values.collect(Collectors.toSet());
                                                    switch (items.size()) {
                                                      case 0:
                                                        return DSL.condition(false);
                                                      case 1:
                                                        return field.eq(items.iterator().next());
                                                      default:
                                                        return field.eq(
                                                            DSL.any(items.toArray(String[]::new)));
                                                    }
                                                  }

                                                  @Override
                                                  public Condition not(Condition clause) {
                                                    return clause.not();
                                                  }

                                                  @Override
                                                  public Condition of(boolean value) {
                                                    return DSL.condition(value);
                                                  }

                                                  @Override
                                                  public Condition or(Stream<Condition> clauses) {
                                                    return clauses
                                                        .reduce(Condition::or)
                                                        .orElseGet(() -> DSL.condition(false));
                                                  }

                                                  @Override
                                                  public Condition workflowId(Stream<String> ids) {
                                                    return match(WORKFLOW_VERSION.HASH_ID, ids);
                                                  }

                                                  @Override
                                                  public Condition workflowName(
                                                      Stream<String> names) {
                                                    return match(WORKFLOW_VERSION.NAME, names);
                                                  }

                                                  @Override
                                                  public Condition workflowRunId(
                                                      Stream<String> ids) {
                                                    Stream<String> hashIds =
                                                        ids.map(
                                                            id ->
                                                                processor
                                                                    .extractHashIfIsFullWorkflowRunId(
                                                                        id));
                                                    return match(WORKFLOW_RUN.HASH_ID, hashIds);
                                                  }
                                                })))
                            .fetch(WORKFLOW_RUN.ID));
                if (request.isRecursive()) {
                  Collection<Long> latestWorkflowRunIds = workflowRuns;
                  do {
                    Collection<Long> downstreamWorkflowRunIds =
                        getIdsForWorkflowRunsDownstreamFrom(latestWorkflowRunIds, configuration);
                    latestWorkflowRunIds =
                        DSL.using(configuration)
                            .select(WORKFLOW_RUN.ID)
                            .from(WORKFLOW_RUN)
                            .where(
                                WORKFLOW_RUN
                                    .COMPLETED
                                    .isNotNull()
                                    .and(WORKFLOW_RUN.ID.in(downstreamWorkflowRunIds)))
                            .fetch(WORKFLOW_RUN.ID);
                    workflowRuns.addAll(latestWorkflowRunIds);
                  } while (!latestWorkflowRunIds.isEmpty());
                }
                return handleWorkflowRuns.process(configuration, workflowRuns.toArray(Long[]::new));
              });
    }
  }

  private void updateVersions(HttpServerExchange exchange, BulkVersionRequest request) {
    final int result = processor.updateVersions(request);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, TextFormat.CONTENT_TYPE_004);
    exchange.setStatusCode(StatusCodes.OK);
    try (final OutputStream os = exchange.getOutputStream()) {
      os.write(Integer.toString(result).getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void insertWorkflowDefinition(
      DSLContext dsl,
      WorkflowLanguage language,
      String workflowDefinition,
      String workflowDefinitionHash)
      throws NoSuchAlgorithmException {
    dsl.insertInto(WORKFLOW_DEFINITION)
        .set(WORKFLOW_DEFINITION.HASH_ID, param("hashId", workflowDefinitionHash))
        .set(WORKFLOW_DEFINITION.WORKFLOW_FILE, param("workflowFile", workflowDefinition))
        .set(WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE, param("workflowLanguage", language))
        .onConflict(WORKFLOW_DEFINITION.HASH_ID)
        .doNothing()
        .execute();
  }

  private Map<String, BasicType> upsertWorkflowReturningLabels(
      Configuration configuration, String workflowName, Map<String, BasicType> workflowLabels)
      throws JsonProcessingException {
    String result =
        Optional.ofNullable(
                DSL.using(configuration)
                    .insertInto(WORKFLOW)
                    .set(WORKFLOW.NAME, param("name", workflowName))
                    .set(WORKFLOW.IS_ACTIVE, param("isActive", false))
                    .set(WORKFLOW.MAX_IN_FLIGHT, param("maxInFlight", 0))
                    .set(
                        WORKFLOW.LABELS,
                        param("labels", JSONB.valueOf(MAPPER.writeValueAsString(workflowLabels))))
                    .onConflict(WORKFLOW.NAME)
                    .doUpdate()
                    .set(
                        WORKFLOW.IS_ACTIVE,
                        WORKFLOW.IS_ACTIVE) // We do this pointless update because if we
                    // don't, Postgres will return no rows
                    .returningResult(WORKFLOW.LABELS)
                    .fetchOptional()
                    .orElseThrow()
                    .value1())
            .map(r -> r.data())
            .orElseGet(() -> "{}"); // There are some cases
    // where value1 is present enough to be returned, but null-like enough to throw NPE when
    // calling .data() on it.

    return MAPPER.readValue(result, new TypeReference<>() {});
  }

  private String generateWorkflowDefinitionHash(String workflowDefinition)
      throws NoSuchAlgorithmException {
    return hexDigits(
        MessageDigest.getInstance("SHA-256")
            .digest(workflowDefinition.getBytes(StandardCharsets.UTF_8)));
  }

  private String generateWorkflowVersionHash(
      String workflowName,
      String version,
      String workflowDefinitionHash,
      Map<String, OutputType> outputs,
      Map<String, InputType> inputs,
      Map<String, String> accessoryHashes)
      throws NoSuchAlgorithmException, JsonProcessingException {
    final MessageDigest versionDigest = MessageDigest.getInstance("SHA-256");
    versionDigest.update(workflowName.getBytes(StandardCharsets.UTF_8));
    versionDigest.update(new byte[] {0});
    versionDigest.update(version.getBytes(StandardCharsets.UTF_8));
    versionDigest.update(new byte[] {0});
    versionDigest.update(workflowDefinitionHash.getBytes(StandardCharsets.UTF_8));
    versionDigest.update(MAPPER.writeValueAsBytes(outputs));
    versionDigest.update(MAPPER.writeValueAsBytes(inputs));

    if (accessoryHashes != null) {
      for (final Entry<String, String> accessory : new TreeMap<>(accessoryHashes).entrySet()) {
        versionDigest.update(new byte[] {0});
        versionDigest.update(accessory.getKey().getBytes(StandardCharsets.UTF_8));
        versionDigest.update(new byte[] {0});
        versionDigest.update(accessory.getValue().getBytes(StandardCharsets.UTF_8));
      }
    }
    return hexDigits(versionDigest.digest());
  }

  private void insertWorkflowAccessoryFiles(
      Map<String, String> accessoryFiles,
      Map<String, String> accessoryHashes,
      WorkflowLanguage workflowLanguage,
      DSLContext dsl)
      throws NoSuchAlgorithmException {
    for (final Entry<String, String> accessory : new TreeMap<>(accessoryFiles).entrySet()) {
      insertWorkflowDefinition(
          dsl, workflowLanguage, accessory.getValue(), accessoryHashes.get(accessory.getKey()));
    }
  }

  private Collection<Long> getIdsForWorkflowRunsDownstreamFrom(
      Collection<Long> workflowRunIds, Configuration configuration) {
    workflowRunIds = workflowRunIds.stream().filter(Objects::nonNull).collect(Collectors.toList());
    Long[] wfr = workflowRunIds.toArray(new Long[workflowRunIds.size()]);

    GetIdsForDownstreamWorkflowRuns getIdsForDownstreamWorkflowRuns =
        new GetIdsForDownstreamWorkflowRuns();
    Result<Record> records =
        DSL.using(configuration).select().from(getIdsForDownstreamWorkflowRuns.call(wfr)).fetch();
    return records.getValues(getIdsForDownstreamWorkflowRuns.WFR_ID);
  }

  private void okEmptyResponse(HttpServerExchange exchange) {
    exchange.setStatusCode(StatusCodes.OK);
    exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
    exchange.getResponseSender().send("");
  }

  private void okJsonResponse(HttpServerExchange exchange, String json) {
    exchange.setStatusCode(StatusCodes.OK);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
    exchange.getResponseSender().send(json);
  }

  private void createdResponse(HttpServerExchange exchange) {
    exchange.setStatusCode(StatusCodes.CREATED);
    exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
    exchange.getResponseSender().send("");
  }

  private void badRequestResponse(HttpServerExchange exchange, String message) {
    exchange.setStatusCode(StatusCodes.BAD_REQUEST);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
    exchange.getResponseSender().send(message);
  }

  private void notFoundResponse(HttpServerExchange exchange) {
    notFoundResponse(exchange, "");
  }

  private void notFoundResponse(HttpServerExchange exchange, String message) {
    exchange.setStatusCode(StatusCodes.NOT_FOUND);
    exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
    exchange.getResponseSender().send(message);
  }

  private void conflictResponse(HttpServerExchange exchange) {
    exchange.setStatusCode(StatusCodes.CONFLICT);
    exchange.getResponseHeaders().add(Headers.CONTENT_LENGTH, 0);
    exchange.getResponseSender().send("");
  }

  private void internalServerErrorResponse(HttpServerExchange exchange, Exception e) {
    e.printStackTrace();
    exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
    exchange.getResponseSender().send(e.getMessage());
  }
}
