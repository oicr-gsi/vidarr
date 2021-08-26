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
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import ca.on.oicr.gsi.vidarr.api.AddWorkflowRequest;
import ca.on.oicr.gsi.vidarr.api.AddWorkflowVersionRequest;
import ca.on.oicr.gsi.vidarr.api.AnalysisOutputType;
import ca.on.oicr.gsi.vidarr.api.AnalysisProvenanceRequest;
import ca.on.oicr.gsi.vidarr.api.BulkVersionRequest;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ExternalMultiVersionKey;
import ca.on.oicr.gsi.vidarr.api.InFlightCountsByWorkflow;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.api.SubmitMode;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowRequest;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponse;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseConflict;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseDryRun;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseFailure;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseMissingKeyVersions;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseSuccess;
import ca.on.oicr.gsi.vidarr.api.UnloadRequest;
import ca.on.oicr.gsi.vidarr.api.UnloadedData;
import ca.on.oicr.gsi.vidarr.api.UnloadedWorkflow;
import ca.on.oicr.gsi.vidarr.api.UnloadedWorkflowVersion;
import ca.on.oicr.gsi.vidarr.api.VersionPolicy;
import ca.on.oicr.gsi.vidarr.core.BaseProcessor;
import ca.on.oicr.gsi.vidarr.core.ExtractInputVidarrIds;
import ca.on.oicr.gsi.vidarr.core.FileMetadata;
import ca.on.oicr.gsi.vidarr.core.OperationStatus;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.DeleteResultHandler;
import ca.on.oicr.gsi.vidarr.server.dto.ServerConfiguration;
import ca.on.oicr.gsi.vidarr.server.jooq.tables.ExternalIdVersion;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.util.Headers;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
import org.flywaydb.core.Flyway;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSON;
import org.jooq.JSONB;
import org.jooq.JSONEntry;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.postgresql.ds.PGSimpleDataSource;

public final class Main implements ServerConfig {

  private interface JsonPost<T> {
    static <T> HttpHandler parse(Class<T> clazz, JsonPost<T> handler) {
      return exchange ->
          exchange
              .getRequestReceiver()
              .receiveFullBytes(
                  (e, data) -> {
                    try {
                      handler.handleRequest(exchange, MAPPER.readValue(data, clazz));
                    } catch (IOException exception) {
                      exception.printStackTrace();
                      e.setStatusCode(StatusCodes.BAD_REQUEST);
                      e.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
                      e.getResponseSender().send(exception.getMessage());
                    }
                  });
    }

    void handleRequest(HttpServerExchange exchange, T body);
  }

  private interface UnloadProcessor<T> {
    T process(Configuration configuration, Integer[] workflowRuns) throws IOException, SQLException;
  }

  static final HttpClient CLIENT =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .followRedirects(HttpClient.Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(20))
          .build();
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final JsonFactory MAPPER_FACTORY = new JsonFactory().setCodec(MAPPER);
  private static final String CONTENT_TYPE_TEXT = "text/plain";
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final Counter REMOTE_ERROR_COUNT =
      Counter.build(
              "vidarr_remote_vidarr_error_count",
              "The number of times a remote instance returned an error.")
          .labelNames("remote")
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
                    .orderBy(externalVersionId.CREATED.desc()),
            allowedTypes);
      default:
        return DSL.inline(null, SQLDataType.JSON);
    }
  }

  private static Field<?> createQueryOnVersion(
      Function<ExternalIdVersion, Field<?>> fieldConstructor, Set<String> allowedTypes) {
    var condition = EXTERNAL_ID.ID.eq(EXTERNAL_ID_VERSION.EXTERNAL_ID_ID);
    if (allowedTypes != null) {
      condition = condition.and(EXTERNAL_ID_VERSION.KEY.in(allowedTypes));
    }
    final var externalIdVersionAlias = EXTERNAL_ID_VERSION.as("externalIdVersionInner");
    final var table =
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
    final var e = (Exception) exchange.getAttachment(ExceptionHandler.THROWABLE);
    if (e instanceof ValidationException) {
      exchange.setStatusCode(StatusCodes.BAD_REQUEST);
    } else {
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
    }
    e.printStackTrace();
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
    final var main = new Main(MAPPER.readValue(new File(args[0]), ServerConfiguration.class));
    startServer(main);
  }

  protected static void startServer(Main server) throws SQLException {
    final var undertow =
        Undertow.builder()
            .addHttpListener(server.port, "0.0.0.0")
            .setWorkerThreads(server.dataSource.getMaximumPoolSize())
            .setHandler(
                Handlers.exceptionHandler(
                        Handlers.routing()
                            .get("/", monitor(new BlockingHandler(server::status)))
                            .get("/api/file/{hash}", monitor(server::fetchFile))
                            .get("/api/run/{hash}", monitor(new BlockingHandler(server::fetchRun)))
                            .get(
                                "/api/status", monitor(new BlockingHandler(server::fetchAllActive)))
                            .get("/api/status/{hash}", monitor(server::fetchStatus))
                            .get("/api/targets", monitor(server::fetchTargets))
                            .get("/api/url/{hash}", monitor(server::fetchUrl))
                            .get("/api/waiting", monitor(server::fetchWaiting))
                            .get("/api/workflows", monitor(server::fetchWorkflows))
                            .get(
                                "/api/max-in-flight",
                                monitor(new BlockingHandler(server::fetchMaxInFlight)))
                            .get("/metrics", monitor(new BlockingHandler(Main::metrics)))
                            .post(
                                "/api/provenance",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(
                                            AnalysisProvenanceRequest.class,
                                            server::fetchProvenance))))
                            .post(
                                "/api/copy-out",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(UnloadRequest.class, server::copyOut))))
                            .post(
                                "/api/unload",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(UnloadRequest.class, server::unload))))
                            .post(
                                "/api/load",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(UnloadedData.class, server::load))))
                            .delete(
                                "/api/status/{hash}",
                                monitor(new BlockingHandler(server::deleteWorkflowRun)))
                            .post(
                                "/api/submit",
                                monitor(
                                    JsonPost.parse(SubmitWorkflowRequest.class, server::submit)))
                            .get(
                                "/api/workflow/{name}",
                                monitor(new BlockingHandler(server::fetchWorkflow)))
                            .post(
                                "/api/workflow/{name}",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(
                                            AddWorkflowRequest.class, server::upsertWorkflow))))
                            .delete(
                                "/api/workflow/{name}",
                                monitor(new BlockingHandler(server::disableWorkflow)))
                            .get(
                                "/api/workflow/{name}/{version}",
                                monitor(new BlockingHandler(server::fetchWorkflowVersion)))
                            .post(
                                "/api/workflow/{name}/{version}",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(
                                            AddWorkflowVersionRequest.class,
                                            server::addWorkflowVersion))))
                            .post(
                                "/api/versions",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(
                                            BulkVersionRequest.class, server::updateVersions))))
                            .setFallbackHandler(
                                new ResourceHandler(
                                    new ClassPathResourceManager(
                                        server.getClass().getClassLoader(),
                                        server.getClass().getPackage()))))
                    .addExceptionHandler(Exception.class, Main::handleException))
            .build();
    undertow.start();
    server.recover();
  }

  private static void metrics(HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, TextFormat.CONTENT_TYPE_004);
    exchange.setStatusCode(StatusCodes.OK);
    try (final var os = exchange.getOutputStream();
        final var writer = new PrintWriter(os)) {
      TextFormat.write004(writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static HttpHandler monitor(HttpHandler handler) {
    return exchange -> {
      final var url = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
      try (final var ignored =
          RESPONSE_TIME.start(url == null ? "unknown" : url.getMatchedTemplate())) {
        handler.handleRequest(exchange);
      }
    };
  }

  private final HikariDataSource dataSource;
  private long epoch = ManagementFactory.getRuntimeMXBean().getStartTime();
  private final ReentrantReadWriteLock epochLock = new ReentrantReadWriteLock();
  private final ScheduledExecutorService executor =
      Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
  private final Map<String, InputProvisioner> inputProvisioners;
  private final Semaphore loadCounter = new Semaphore(3);
  private final MaxInFlightByWorkflow maxInFlightPerWorkflow = new MaxInFlightByWorkflow();
  private final Map<String, Optional<FileMetadata>> metadataCache = new ConcurrentHashMap<>();
  private final Map<String, String> otherServers;
  private final Map<String, OutputProvisioner> outputProvisioners;
  private final int port;
  private final DatabaseBackedProcessor processor;
  private final Map<String, RuntimeProvisioner> runtimeProvisioners;
  private final String selfName;
  private final String selfUrl;
  private final Map<String, Target> targets;
  private final Path unloadDirectory;
  private final Map<String, WorkflowEngine> workflowEngines;
  private final StatusPage status =
      new StatusPage(this) {
        @Override
        protected void emitCore(SectionRenderer sectionRenderer) {
          sectionRenderer.line("Self-URL", selfUrl);
          for (final var target : targets.keySet()) {
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
    selfUrl = configuration.getUrl();
    selfName = configuration.getName();
    port = configuration.getPort();
    otherServers = configuration.getOtherServers();
    workflowEngines = configuration.getWorkflowEngines();
    inputProvisioners = configuration.getInputProvisioners();
    outputProvisioners = configuration.getOutputProvisioners();
    runtimeProvisioners = configuration.getRuntimeProvisioners();

    for (final var input : inputProvisioners.values()) {
      input.startup();
    }
    for (final var output : outputProvisioners.values()) {
      output.startup();
    }
    for (final var runtime : runtimeProvisioners.values()) {
      runtime.startup();
    }
    for (final var engine : workflowEngines.values()) {
      engine.startup();
    }
    final var consumableResources = configuration.getConsumableResources();
    for (final var resource : consumableResources.entrySet()) {
      resource.getValue().startup(resource.getKey());
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
                                              "", maxInFlightPerWorkflow)))
                                  .collect(Collectors.toList());
                          private final WorkflowEngine engine =
                              workflowEngines.get(e.getValue().getWorkflowEngine());
                          private final Map<InputProvisionFormat, InputProvisioner>
                              inputProvisioners =
                                  e.getValue().getInputProvisioners().stream()
                                      .map(Main.this.inputProvisioners::get)
                                      .flatMap(
                                          p ->
                                              Stream.of(InputProvisionFormat.values())
                                                  .filter(p::canProvision)
                                                  .map(f -> new Pair<>(f, p)))
                                      .collect(
                                          Collectors
                                              .<Pair<InputProvisionFormat, InputProvisioner>,
                                                  InputProvisionFormat, InputProvisioner>
                                                  toMap(Pair::first, Pair::second));
                          private final Map<OutputProvisionFormat, OutputProvisioner>
                              outputProvisioners =
                                  e.getValue().getOutputProvisioners().stream()
                                      .map(Main.this.outputProvisioners::get)
                                      .flatMap(
                                          p ->
                                              Stream.of(OutputProvisionFormat.values())
                                                  .filter(p::canProvision)
                                                  .map(f -> new Pair<>(f, p)))
                                      .collect(
                                          Collectors
                                              .<Pair<OutputProvisionFormat, OutputProvisioner>,
                                                  OutputProvisionFormat, OutputProvisioner>
                                                  toMap(Pair::first, Pair::second));
                          private final List<RuntimeProvisioner> runtimeProvisioners =
                              e.getValue().getRuntimeProvisioners().stream()
                                  .map(Main.this.runtimeProvisioners::get)
                                  .collect(Collectors.toList());

                          @Override
                          public Stream<Pair<String, ConsumableResource>> consumableResources() {
                            return consumables.stream();
                          }

                          @Override
                          public WorkflowEngine engine() {
                            return engine;
                          }

                          @Override
                          public InputProvisioner provisionerFor(InputProvisionFormat type) {
                            return inputProvisioners.get(type);
                          }

                          @Override
                          public OutputProvisioner provisionerFor(OutputProvisionFormat type) {
                            return outputProvisioners.get(type);
                          }

                          @Override
                          public Stream<RuntimeProvisioner> runtimeProvisioners() {
                            return runtimeProvisioners.stream();
                          }
                        }));

    final var simpleConnection = new PGSimpleDataSource();
    simpleConnection.setServerNames(new String[] {configuration.getDbHost()});
    simpleConnection.setPortNumbers(new int[] {configuration.getDbPort()});
    simpleConnection.setDatabaseName(configuration.getDbName());
    simpleConnection.setUser(configuration.getDbUser());
    simpleConnection.setPassword(configuration.getDbPass());
    var fw = Flyway.configure().dataSource(simpleConnection);
    fw.locations("classpath:db/migration").load().migrate();

    final var config = new HikariConfig();
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
            final var match = BaseProcessor.ANALYSIS_RECORD_ID.matcher(id);
            if (!match.matches() || !match.group("type").equals("file")) {
              return Optional.empty();
            }
            final var instance = match.group("instance");
            final var hash = match.group("hash");
            if (instance.equals("_") || instance.equals(selfName)) {
              return resolveInDatabase(hash);
            }
            final var remote = otherServers.get(instance);
            if (remote == null) {
              return Optional.empty();
            }

            try (final var ignored = REMOTE_RESPONSE_TIME.start(remote)) {
              final var response =
                  CLIENT.send(
                      HttpRequest.newBuilder()
                          .uri(URI.create(String.format("%s/api/file/%s", remote, hash)))
                          .timeout(Duration.ofMinutes(1))
                          .GET()
                          .build(),
                      new JsonBodyHandler<>(
                          MAPPER, new TypeReference<ProvenanceAnalysisRecord<ExternalKey>>() {}));
              if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                final var result = response.body().get();
                return Optional.of(
                    new FileMetadata() {
                      private final List<ExternalKey> keys = result.getExternalKeys();
                      private final String path = result.getPath();

                      @Override
                      public Stream<ExternalKey> externalKeys() {
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
            return metadataCache.computeIfAbsent(id, this::fetchPathForId);
          }

          @Override
          protected Optional<Target> targetByName(String name) {
            return Optional.ofNullable(targets.get(name));
          }
        };

    try (final var connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .select(WORKFLOW.NAME, WORKFLOW.MAX_IN_FLIGHT)
          .from(WORKFLOW)
          .forEach(record -> maxInFlightPerWorkflow.set(record.value1(), record.value2()));
    }
    unloadDirectory = Path.of(configuration.getUnloadDirectory());
  }

  private void upsertWorkflow(HttpServerExchange exchange, AddWorkflowRequest request) {
    final var name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");

    try (final var connection = dataSource.getConnection()) {
      final var labels = JSONB.valueOf(MAPPER.writeValueAsString(request.getLabels()));
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              context ->
                  DSL.using(context)
                      .insertInto(WORKFLOW)
                      .columns(
                          WORKFLOW.NAME,
                          WORKFLOW.LABELS,
                          WORKFLOW.IS_ACTIVE,
                          WORKFLOW.MAX_IN_FLIGHT)
                      .values(name, labels, true, request.getMaxInFlight())
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
      exchange.setStatusCode(StatusCodes.CREATED);
      exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
      exchange.getResponseSender().send("");
    } catch (SQLException | JsonProcessingException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void addWorkflowVersion(HttpServerExchange exchange, AddWorkflowVersionRequest request)
      throws ValidationException {
    final var name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");
    final var version =
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
      validationErrors.add(ValidationError.forRequired("workflow"));
    }
    if (request.getLanguage() == null) {
      validationErrors.add(ValidationError.forRequired("language"));
    }
    if (!validationErrors.isEmpty()) {
      exchange.setStatusCode(StatusCodes.BAD_REQUEST);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(validationErrors.toString());
      return;
    }
    try (final var connection = dataSource.getConnection()) {
      DSL.using(connection, SQLDialect.POSTGRES)
          .transaction(
              context -> {
                final var dsl = DSL.using(context);
                var matchingWorkflow =
                    dsl.select(WORKFLOW.ID)
                        .from(WORKFLOW)
                        .where(WORKFLOW.NAME.eq(name))
                        .fetchOptional(Record1::value1);
                if (matchingWorkflow.isEmpty()) {
                  exchange.setStatusCode(StatusCodes.NOT_FOUND);
                  exchange
                      .getResponseSender()
                      .send(String.format("No workflow with name %s found", name));
                  return;
                }
                final String definitionHash =
                    upsertWorkflowDefinition(dsl, request.getLanguage(), request.getWorkflow());
                final var versionDigest = MessageDigest.getInstance("SHA-256");
                versionDigest.update(name.getBytes(StandardCharsets.UTF_8));
                versionDigest.update(new byte[] {0});
                versionDigest.update(version.getBytes(StandardCharsets.UTF_8));
                versionDigest.update(new byte[] {0});
                versionDigest.update(definitionHash.getBytes(StandardCharsets.UTF_8));
                versionDigest.update(MAPPER.writeValueAsBytes(request.getOutputs()));
                versionDigest.update(MAPPER.writeValueAsBytes(request.getParameters()));

                final var accessoryHashes = new TreeMap<String, String>();
                for (final var accessory : new TreeMap<>(request.getAccessoryFiles()).entrySet()) {
                  final var accessoryHash =
                      upsertWorkflowDefinition(dsl, request.getLanguage(), accessory.getValue());
                  versionDigest.update(new byte[] {0});
                  versionDigest.update(accessory.getKey().getBytes(StandardCharsets.UTF_8));
                  versionDigest.update(new byte[] {0});
                  versionDigest.update(accessoryHash.getBytes(StandardCharsets.UTF_8));
                  accessoryHashes.put(accessory.getKey(), accessoryHash);
                }
                final var versionHash = hexDigits(versionDigest.digest());
                final var result =
                    dsl.insertInto(
                            WORKFLOW_VERSION,
                            WORKFLOW_VERSION.HASH_ID,
                            WORKFLOW_VERSION.METADATA,
                            WORKFLOW_VERSION.NAME,
                            WORKFLOW_VERSION.PARAMETERS,
                            WORKFLOW_VERSION.WORKFLOW_DEFINITION,
                            WORKFLOW_VERSION.VERSION)
                        .values(
                            DSL.val(versionHash),
                            DSL.val(
                                MAPPER.valueToTree(request.getOutputs()),
                                WORKFLOW_VERSION.METADATA.getDataType()),
                            DSL.val(name),
                            DSL.val(
                                MAPPER.valueToTree(request.getParameters()),
                                WORKFLOW_VERSION.PARAMETERS.getDataType()),
                            DSL.field(
                                DSL.select(WORKFLOW_DEFINITION.ID)
                                    .from(WORKFLOW_DEFINITION)
                                    .where(WORKFLOW_DEFINITION.HASH_ID.eq(definitionHash))),
                            DSL.val(version))
                        .onConflict(WORKFLOW_VERSION.NAME, WORKFLOW_VERSION.VERSION)
                        .doNothing()
                        .returningResult(DSL.field(WORKFLOW_VERSION.HASH_ID.eq(versionHash)))
                        .fetchOptional();
                if (result.map(r -> !r.value1()).orElse(false)) {
                  exchange.setStatusCode(StatusCodes.CONFLICT);
                  exchange.getResponseHeaders().add(Headers.CONTENT_LENGTH, 0);
                  return;
                }
                dsl.update(WORKFLOW)
                    .set(WORKFLOW.IS_ACTIVE, true)
                    .where(WORKFLOW.NAME.eq(name).and(WORKFLOW.IS_ACTIVE.isFalse()))
                    .execute();
                if (!accessoryHashes.isEmpty()) {

                  var accessoryQuery =
                      dsl.insertInto(
                          WORKFLOW_VERSION_ACCESSORY,
                          WORKFLOW_VERSION_ACCESSORY.WORKFLOW_VERSION,
                          WORKFLOW_VERSION_ACCESSORY.FILENAME,
                          WORKFLOW_VERSION_ACCESSORY.WORKFLOW_DEFINITION);

                  for (final var accessory : accessoryHashes.entrySet()) {
                    accessoryQuery =
                        accessoryQuery.values(
                            DSL.field(
                                DSL.select(WORKFLOW_VERSION.ID)
                                    .from(WORKFLOW_VERSION)
                                    .where(
                                        WORKFLOW_VERSION
                                            .NAME
                                            .eq(name)
                                            .and(WORKFLOW_VERSION.VERSION.eq(version)))),
                            DSL.val(accessory.getKey()),
                            DSL.field(
                                DSL.select(WORKFLOW_DEFINITION.ID)
                                    .from(WORKFLOW_DEFINITION)
                                    .where(WORKFLOW_DEFINITION.HASH_ID.eq(accessory.getValue()))));
                  }
                  accessoryQuery.execute();
                }
                exchange.setStatusCode(StatusCodes.CREATED);
                exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
                exchange.getResponseSender().send("");
              });
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void associateDefinitionAsAccessory(
      Configuration configuration, int id, String fileName, String accessoryWorkflowHash) {
    DSL.using(configuration)
        .insertInto(WORKFLOW_VERSION_ACCESSORY)
        .set(WORKFLOW_VERSION_ACCESSORY.FILENAME, fileName)
        .set(
            WORKFLOW_VERSION_ACCESSORY.WORKFLOW_DEFINITION,
            DSL.select(WORKFLOW_DEFINITION.ID)
                .from(WORKFLOW_DEFINITION)
                .where(WORKFLOW_DEFINITION.HASH_ID.eq(accessoryWorkflowHash)))
        .set(WORKFLOW_VERSION_ACCESSORY.WORKFLOW_VERSION, id)
        .execute();
  }

  private void copyOut(HttpServerExchange exchange, UnloadRequest request) {
    try {
      unloadSearch(
          request,
          (tx, ids) -> {
            exchange.setStatusCode(StatusCodes.OK);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
            try (final var output = MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
              dumpUnloadDataToJson(tx, ids, output);
            }
            return null;
          });
    } catch (Exception e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private Field<JSON> createAnalysisJsonField(Field<JSON> externalKeys, JSONEntry<?>... extra) {
    final var analysisCommonFields = new ArrayList<>(List.of(extra));
    analysisCommonFields.add(literalJsonEntry("id", ANALYSIS.HASH_ID));
    analysisCommonFields.add(literalJsonEntry("type", ANALYSIS.ANALYSIS_TYPE));
    analysisCommonFields.add(literalJsonEntry("created", ANALYSIS.CREATED));
    analysisCommonFields.add(literalJsonEntry("labels", ANALYSIS.LABELS));
    analysisCommonFields.add(literalJsonEntry("modified", ANALYSIS.MODIFIED));
    analysisCommonFields.add(literalJsonEntry("externalKeys", externalKeys));

    final var analysisFileFields = new ArrayList<>(analysisCommonFields);
    analysisFileFields.add(literalJsonEntry("path", ANALYSIS.FILE_PATH));
    analysisFileFields.add(literalJsonEntry("md5", ANALYSIS.FILE_MD5SUM));
    analysisFileFields.add(literalJsonEntry("metatype", ANALYSIS.FILE_METATYPE));
    analysisFileFields.add(literalJsonEntry("size", ANALYSIS.FILE_SIZE));

    final var analysisUrlFields = new ArrayList<>(analysisCommonFields);
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
    final var fields = new ArrayList<JSONEntry<?>>();

    fields.add(literalJsonEntry("completed", WORKFLOW_RUN.COMPLETED));
    fields.add(literalJsonEntry("created", WORKFLOW_RUN.CREATED));
    fields.add(literalJsonEntry("id", WORKFLOW_RUN.HASH_ID));
    fields.add(literalJsonEntry("inputFiles", WORKFLOW_RUN.INPUT_FILE_IDS));
    fields.add(literalJsonEntry("labels", WORKFLOW_RUN.LABELS));
    fields.add(literalJsonEntry("modified", WORKFLOW_RUN.MODIFIED));
    fields.add(literalJsonEntry("started", WORKFLOW_RUN.STARTED));

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
    final var vidarrId =
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
    final var name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");

    try (final var connection = dataSource.getConnection()) {
      final var count =
          DSL.using(connection, SQLDialect.POSTGRES)
              .transactionResult(
                  context ->
                      DSL.using(context)
                          .update(WORKFLOW)
                          .set(WORKFLOW.IS_ACTIVE, false)
                          .where(WORKFLOW.NAME.eq(name).and(WORKFLOW.IS_ACTIVE.isTrue()))
                          .execute());
      exchange.setStatusCode(count == 0 ? StatusCodes.NOT_FOUND : StatusCodes.OK);
      exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
      exchange.getResponseSender().send("");
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void dumpUnloadDataToJson(Configuration tx, Integer[] ids, JsonGenerator output)
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
    final var accessoryDefinition = WORKFLOW_DEFINITION.as("accessoryWorkflowDefinition");
    DSL.using(tx)
        .select(
            DSL.jsonObject(
                literalJsonEntry(
                    "accessoryFiles",
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
                                            WORKFLOW_VERSION.ID))))),
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

    try (final var connection = dataSource.getConnection();
        final var output = MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
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
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchAnalysis(HttpServerExchange exchange, String type) {
    final var vidarrId =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("hash");
    try (final var connection = dataSource.getConnection()) {
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
          .where(ANALYSIS.HASH_ID.eq(vidarrId).and(ANALYSIS.ANALYSIS_TYPE.eq(type)))
          .fetchOptional()
          .map(Record1::value1)
          .ifPresentOrElse(
              result -> {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
                exchange.setStatusCode(StatusCodes.OK);
                exchange.getResponseSender().send(result.data());
              },
              () -> {
                exchange.setStatusCode(StatusCodes.NOT_FOUND);
                exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
                exchange.getResponseSender().send("");
              });
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchFile(HttpServerExchange exchange) {
    fetchAnalysis(exchange, "file");
  }

  private void fetchMaxInFlight(HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
    exchange.setStatusCode(StatusCodes.OK);
    final var endTime = OffsetDateTime.now();
    try (final var output = MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
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
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchProvenance(HttpServerExchange exchange, AnalysisProvenanceRequest request) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
    exchange.setStatusCode(StatusCodes.OK);
    final var endTime = OffsetDateTime.now();
    epochLock.readLock().lock();
    try (final var output = MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
      if (request.getEpoch() != epoch) {
        request.setTimestamp(0);
      }
      output.writeStartObject();
      output.writeNumberField("epoch", epoch);
      output.writeNumberField("timestamp", endTime.toInstant().toEpochMilli());
      output.writeArrayFieldStart("results");
      try (final var connection = dataSource.getConnection()) {
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
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    } finally {
      epochLock.readLock().unlock();
    }
  }

  private void fetchRun(HttpServerExchange exchange) {
    try (final var connection = dataSource.getConnection()) {
      final var vidarrId =
          exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("hash");
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
      DSL.using(connection, SQLDialect.POSTGRES)
          .select(DSL.field(ACTIVE_WORKFLOW_RUN.ID.isNull()))
          .from(
              WORKFLOW_RUN
                  .leftJoin(ACTIVE_WORKFLOW_RUN)
                  .on(WORKFLOW_RUN.ID.eq(ACTIVE_WORKFLOW_RUN.ID)))
          .where(WORKFLOW_RUN.HASH_ID.eq(vidarrId))
          .fetchOptional(Record1::value1)
          .ifPresentOrElse(
              complete -> {
                exchange.setStatusCode(StatusCodes.OK);
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
                try (final var output =
                    MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
                  createAnalysisRecords(
                      DSL.using(connection, SQLDialect.POSTGRES),
                      output,
                      VersionPolicy.ALL,
                      null,
                      true,
                      Set.of(AnalysisOutputType.FILE, AnalysisOutputType.URL),
                      WORKFLOW_RUN.HASH_ID.eq(vidarrId));
                } catch (IOException | SQLException e) {
                  e.printStackTrace();
                }
              },
              () -> {
                exchange.setStatusCode(StatusCodes.NOT_FOUND);
                exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
                exchange.getResponseSender().send("");
              });
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchStatus(HttpServerExchange exchange) {
    try (final var connection = dataSource.getConnection()) {
      final var vidarrId =
          exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("hash");
      DSL.using(connection, SQLDialect.POSTGRES)
          .select(DSL.jsonObject(STATUS_FIELDS))
          .from(
              WORKFLOW_RUN
                  .leftJoin(ACTIVE_WORKFLOW_RUN)
                  .on(WORKFLOW_RUN.ID.eq(ACTIVE_WORKFLOW_RUN.ID)))
          .where(WORKFLOW_RUN.HASH_ID.eq(vidarrId))
          .fetchOptional(Record1::value1)
          .ifPresentOrElse(
              record -> {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
                exchange.setStatusCode(StatusCodes.OK);
                exchange.getResponseSender().send(record.data());
              },
              () -> {
                exchange.setStatusCode(StatusCodes.NOT_FOUND);
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
                exchange.getResponseSender().send("");
              });
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchTargets(HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
    exchange.setStatusCode(StatusCodes.OK);
    final var targetsOutput = MAPPER.createObjectNode();
    for (final var target : targets.entrySet()) {
      final var targetOutput = targetsOutput.putObject(target.getKey());
      final var languages = targetOutput.putArray("language");
      for (final var language : WorkflowLanguage.values()) {
        if (target.getValue().engine().supports(language)) {
          languages.add(language.name());
        }
      }
      targetOutput.putPOJO(
          "engineParameters", target.getValue().engine().engineParameters().orElse(null));
      final var inputProvisioners = targetOutput.putObject("inputProvisioners");
      for (final var inputFormat : InputProvisionFormat.values()) {
        final var provisioner = target.getValue().provisionerFor(inputFormat);
        if (provisioner != null) {
          inputProvisioners.putPOJO(inputFormat.name(), provisioner.externalTypeFor(inputFormat));
        }
      }
      final var outputProvisioners = targetOutput.putObject("outputProvisioners");
      for (final var outputFormat : OutputProvisionFormat.values()) {
        final var provisioner = target.getValue().provisionerFor(outputFormat);
        if (provisioner != null) {
          outputProvisioners.putPOJO(outputFormat.name(), provisioner.typeFor(outputFormat));
        }
      }
      final var consumableResources = targetOutput.putObject("consumableResources");
      target
          .getValue()
          .consumableResources()
          .flatMap(cr -> cr.second().inputFromSubmitter().stream())
          .forEach(cr -> consumableResources.putPOJO(cr.first(), cr.second()));
    }
    try {
      exchange.getResponseSender().send(MAPPER.writeValueAsString(targetsOutput));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private void fetchUrl(HttpServerExchange exchange) {
    fetchAnalysis(exchange, "url");
  }

  private void fetchWaiting(HttpServerExchange exchange) {
    try (final var connection = dataSource.getConnection()) {
      var result =
          DSL.using(connection, SQLDialect.POSTGRES)
              .select(
                  DSL.jsonArrayAgg(
                      DSL.jsonObject(
                          literalJsonEntry("workflow", WORKFLOW_VERSION.NAME),
                          literalJsonEntry(
                              "oldest",
                              DSL.field(
                                  DSL.select(DSL.min(WORKFLOW_RUN.CREATED))
                                      .from(WORKFLOW_RUN)
                                      .where(
                                          WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(
                                              WORKFLOW_VERSION.ID)))),
                          literalJsonEntry(
                              "workflowRuns",
                              DSL.field(
                                  DSL.select(DSL.jsonArrayAgg(WORKFLOW_RUN.HASH_ID))
                                      .from(WORKFLOW_RUN)
                                      .where(
                                          WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(
                                              WORKFLOW_VERSION.ID)))))))
              .from(
                  WORKFLOW_RUN
                      .join(WORKFLOW_VERSION)
                      .on(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID))
                      .join(ACTIVE_WORKFLOW_RUN)
                      .on(WORKFLOW_RUN.ID.eq(ACTIVE_WORKFLOW_RUN.ID)))
              .where(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE.eq(Phase.WAITING_FOR_RESOURCES))
              .groupBy(WORKFLOW_VERSION.NAME)
              .fetchOptional(Record1::value1);
      exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
      exchange.setStatusCode(StatusCodes.OK);
      exchange.getResponseSender().send(result.isPresent() ? result.get().data() : "[]");
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchWorkflows(HttpServerExchange exchange) {
    try (final var connection = dataSource.getConnection()) {
      final var workflowVersionAlias = WORKFLOW_VERSION.as("other_workflow_version");
      final var response =
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
      exchange.setStatusCode(StatusCodes.OK);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
      exchange.getResponseSender().send(response == null ? "[]" : response.data());
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchWorkflow(HttpServerExchange exchange) {
    final var name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");

    try (final var connection = dataSource.getConnection()) {
      final var result =
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
        exchange.setStatusCode(StatusCodes.OK);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
        exchange.getResponseSender().send(result.get());
      } else {
        exchange.setStatusCode(StatusCodes.NOT_FOUND);
        exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
        exchange.getResponseSender().send("");
      }
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchWorkflowVersion(HttpServerExchange exchange) {
    final var name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");
    final var version =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("version");
    try (final var connection = dataSource.getConnection()) {
      final var workflowVersionAlias = WORKFLOW_VERSION.as("other_workflow_version");
      final var result =
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
              .where(WORKFLOW_VERSION.NAME.eq(name).and(WORKFLOW_VERSION.VERSION.eq(version)))
              .fetchOptional()
              .map(r -> r.value1().data());
      if (result.isPresent()) {
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
        exchange.setStatusCode(StatusCodes.OK);
        exchange.getResponseSender().send(result.get());
      } else {
        exchange.setStatusCode(StatusCodes.NOT_FOUND);
        exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
        exchange.getResponseSender().send("");
      }
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  @Override
  public Stream<Header> headers() {
    return Stream.empty();
  }

  private void insertAnalysis(
      Configuration configuration,
      int id,
      ProvenanceAnalysisRecord<ca.on.oicr.gsi.vidarr.api.ExternalId> analysis) {
    final var analysisId =
        DSL.using(configuration)
            .insertInto(ANALYSIS)
            .set(ANALYSIS.WORKFLOW_RUN_ID, id)
            .set(ANALYSIS.HASH_ID, analysis.getId())
            .set(ANALYSIS.ANALYSIS_TYPE, analysis.getType())
            .set(ANALYSIS.CREATED, analysis.getCreated().toOffsetDateTime())
            .set(
                ANALYSIS.FILE_PATH,
                analysis.getType().equals("file") ? analysis.getPath() : analysis.getUrl())
            .set(ANALYSIS.FILE_MD5SUM, analysis.getType().equals("file") ? analysis.getMd5() : null)
            .set(
                ANALYSIS.FILE_METATYPE,
                analysis.getType().equals("file") ? analysis.getMetatype() : null)
            .set(ANALYSIS.FILE_SIZE, analysis.getType().equals("file") ? analysis.getSize() : null)
            .set(ANALYSIS.LABELS, DatabaseWorkflow.labelsToJson(analysis.getLabels()))
            .returningResult(ANALYSIS.ID)
            .fetchOptional()
            .orElseThrow()
            .value1();
    var associate =
        DSL.using(configuration)
            .insertInto(ANALYSIS_EXTERNAL_ID)
            .columns(ANALYSIS_EXTERNAL_ID.ANALYSIS_ID, ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID);
    for (final var externalId : analysis.getExternalKeys()) {
      associate =
          associate.values(
              DSL.val(analysisId),
              DSL.field(
                  DSL.select(EXTERNAL_ID.ID)
                      .from(EXTERNAL_ID)
                      .where(
                          EXTERNAL_ID
                              .WORKFLOW_RUN_ID
                              .eq(id)
                              .and(EXTERNAL_ID.PROVIDER.eq(externalId.getProvider()))
                              .and(EXTERNAL_ID.EXTERNAL_ID_.eq(externalId.getId())))));
    }
    associate.execute();
  }

  private void insertExternalKey(
      Configuration configuration, int id, ExternalMultiVersionKey externalId) {
    final var externalIdDbId =
        DSL.using(configuration)
            .insertInto(EXTERNAL_ID)
            .set(EXTERNAL_ID.PROVIDER, externalId.getProvider())
            .set(EXTERNAL_ID.EXTERNAL_ID_, externalId.getId())
            .set(EXTERNAL_ID.WORKFLOW_RUN_ID, id)
            .returningResult(EXTERNAL_ID.ID)
            .fetchOptional()
            .orElseThrow()
            .value1();
    var insertVersions =
        DSL.using(configuration)
            .insertInto(EXTERNAL_ID_VERSION)
            .columns(
                EXTERNAL_ID_VERSION.EXTERNAL_ID_ID,
                EXTERNAL_ID_VERSION.KEY,
                EXTERNAL_ID_VERSION.VALUE);
    for (final var version : externalId.getVersions().entrySet()) {
      for (final var value : version.getValue()) {
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
        .set(WORKFLOW_DEFINITION.WORKFLOW_FILE, workflowScript)
        .set(WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE, workflowLanguage)
        .set(WORKFLOW_DEFINITION.HASH_ID, rootWorkflowHash)
        .onConflict(WORKFLOW_DEFINITION.HASH_ID)
        .doNothing()
        .execute();
  }

  private Optional<Integer> insertWorkflowRun(
      Configuration configuration,
      int workflowId,
      OffsetDateTime now,
      ca.on.oicr.gsi.vidarr.api.ProvenanceWorkflowRun<ExternalMultiVersionKey> run) {
    return DSL.using(configuration)
        .insertInto(WORKFLOW_RUN)
        .set(WORKFLOW_RUN.HASH_ID, run.getId())
        .set(WORKFLOW_RUN.WORKFLOW_VERSION_ID, workflowId)
        .set(WORKFLOW_RUN.ENGINE_PARAMETERS, run.getEngineParameters())
        .set(WORKFLOW_RUN.ARGUMENTS, run.getArguments())
        .set(WORKFLOW_RUN.METADATA, run.getMetadata())
        .set(WORKFLOW_RUN.LABELS, DatabaseWorkflow.labelsToJson(run.getLabels()))
        .set(WORKFLOW_RUN.INPUT_FILE_IDS, run.getInputFiles().toArray(String[]::new))
        .set(WORKFLOW_RUN.CREATED, run.getCreated().toOffsetDateTime())
        .set(WORKFLOW_RUN.COMPLETED, run.getCompleted().toOffsetDateTime())
        .set(WORKFLOW_RUN.LAST_ACCESSED, now)
        .set(
            WORKFLOW_RUN.STARTED,
            run.getStarted() == null ? null : run.getStarted().toOffsetDateTime())
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
      String workflowHashId,
      String workflowVersion,
      Map<String, OutputType> outputs,
      Map<String, InputType> parameters) {
    return DSL.using(configuration)
        .insertInto(WORKFLOW_VERSION)
        .set(WORKFLOW_VERSION.HASH_ID, workflowHashId)
        .set(WORKFLOW_VERSION.NAME, workflowName)
        .set(WORKFLOW_VERSION.VERSION, workflowVersion)
        .set(WORKFLOW_VERSION.METADATA, MAPPER.<ObjectNode>valueToTree(outputs))
        .set(WORKFLOW_VERSION.PARAMETERS, MAPPER.<ObjectNode>valueToTree(parameters))
        .set(
            WORKFLOW_VERSION.WORKFLOW_DEFINITION,
            DSL.select(WORKFLOW_DEFINITION.ID)
                .from(WORKFLOW_DEFINITION)
                .where(WORKFLOW_DEFINITION.HASH_ID.eq(rootWorkflowHash)))
        .onConflict(WORKFLOW_VERSION.NAME, WORKFLOW_VERSION.VERSION)
        .doNothing()
        .returningResult(WORKFLOW_VERSION.ID)
        .fetchOptional();
  }

  private void load(HttpServerExchange exchange, UnloadedData unloadedData) {
    try {
      // We have to hold a very expensive lock to load data in the database, so we're going to do an
      // offline validation of the data to make sure it's self-consistent, then acquire the lock and
      // do an online validation.
      final var workflowInfo =
          new TreeMap<
              String, Pair<UnloadedWorkflow, Map<String, Pair<String, UnloadedWorkflowVersion>>>>();
      for (final var workflow : unloadedData.getWorkflows()) {
        if (workflowInfo.containsKey(workflow.getName())) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(String.format("Duplicate workflow %s in load request.", workflow.getName()));
          return;
        }
        workflowInfo.put(workflow.getName(), new Pair<>(workflow, new TreeMap<>()));
      }
      for (final var workflowVersion : unloadedData.getWorkflowVersions()) {
        final var info = workflowInfo.get(workflowVersion.getName());
        if (info == null) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(
                  String.format(
                      "Workflow version references unknown workflow %s in load request.",
                      workflowVersion.getName()));
          return;
        }
        if (info.second().containsKey(workflowVersion.getVersion())) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(
                  String.format(
                      "Duplicate workflow version %s/%s in load request.",
                      workflowVersion.getName(), workflowVersion.getVersion()));
          return;
        }

        // Success; compute hash ID and store in map
        final var versionDigest = MessageDigest.getInstance("SHA-256");
        versionDigest.update(workflowVersion.getName().getBytes(StandardCharsets.UTF_8));
        versionDigest.update(new byte[] {0});
        versionDigest.update(workflowVersion.getVersion().getBytes(StandardCharsets.UTF_8));
        versionDigest.update(new byte[] {0});
        versionDigest.update(
            hexDigits(
                    MessageDigest.getInstance("SHA-256")
                        .digest(workflowVersion.getWorkflow().getBytes(StandardCharsets.UTF_8)))
                .getBytes(StandardCharsets.UTF_8));
        versionDigest.update(MAPPER.writeValueAsBytes(workflowVersion.getOutputs()));
        versionDigest.update(MAPPER.writeValueAsBytes(workflowVersion.getParameters()));
        if (workflowVersion.getAccessoryFiles() != null) {
          for (final var accessory :
              new TreeMap<>(workflowVersion.getAccessoryFiles()).entrySet()) {
            final var accessoryHash =
                hexDigits(
                    MessageDigest.getInstance("SHA-256")
                        .digest(accessory.getValue().getBytes(StandardCharsets.UTF_8)));
            versionDigest.update(new byte[] {0});
            versionDigest.update(accessory.getKey().getBytes(StandardCharsets.UTF_8));
            versionDigest.update(new byte[] {0});
            versionDigest.update(accessoryHash.getBytes(StandardCharsets.UTF_8));
          }
        }
        info.second()
            .put(
                workflowVersion.getVersion(),
                new Pair<>(hexDigits(versionDigest.digest()), workflowVersion));
      }

      // Validate the workflow runs; all workflow data must be included, so we don't need the DB for
      // this.
      final var seenWorkflowRunIds = new TreeSet<String>();
      for (final var workflowRun : unloadedData.getWorkflowRuns()) {
        final var info = workflowInfo.get(workflowRun.getWorkflowName());
        if (info == null) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(
                  String.format(
                      "Workflow run %s references missing workflow %s in load request.",
                      workflowRun.getId(), workflowRun.getWorkflowName()));
          return;
        }
        final var versionInfo = info.second().get(workflowRun.getWorkflowVersion());
        if (versionInfo == null) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(
                  String.format(
                      "Workflow run %s references missing workflow %s/%s in load request.",
                      workflowRun.getId(),
                      workflowRun.getWorkflowName(),
                      workflowRun.getWorkflowVersion()));
          return;
        }
        // Validate labels
        final var labelErrors =
            DatabaseBackedProcessor.validateLabels(
                    workflowRun.getLabels(), info.first().getLabels())
                .collect(Collectors.toList());
        if (!labelErrors.isEmpty()) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(
                  String.format(
                      "Workflow run %s has bad labels: %s",
                      workflowRun.getId(), String.join("; ", labelErrors)));
          return;
        }
        final var knownExternalIds =
            workflowRun.getExternalKeys().stream()
                .map(e -> new Pair<>(e.getProvider(), e.getId()))
                .collect(Collectors.toSet());
        if (knownExternalIds.size() != workflowRun.getExternalKeys().size()) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(
                  String.format(
                      "Workflow run %s has duplicate external keys", workflowRun.getId()));
          return;
        }
        // Compute the hash ID this workflow run should have
        final var correctId =
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
                    .map(
                        id -> {
                          Matcher matcher = BaseProcessor.ANALYSIS_RECORD_ID.matcher(id);
                          if (!matcher.matches())
                            throw new IllegalStateException(
                                "Failed to match ANALYSIS_RECORD_ID regex with id: " + id);
                          return matcher.group("hash");
                        })
                    .collect(Collectors.toCollection(TreeSet::new)),
                workflowRun.getExternalKeys());

        if (!correctId.equals(workflowRun.getId())) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(
                  String.format(
                      "Workflow run %s should have ID %s in load request.",
                      workflowRun.getId(), correctId));
          return;
        }
        if (!seenWorkflowRunIds.add(correctId)) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(
                  String.format(
                      "Workflow run %s is included multiple times in the input.",
                      workflowRun.getId()));
          return;
        }
        // Validate output analyses for external IDs and hashes
        if (workflowRun.getAnalysis() == null || workflowRun.getAnalysis().isEmpty()) {
          exchange.setStatusCode(StatusCodes.BAD_REQUEST);
          exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
          exchange
              .getResponseSender()
              .send(String.format("Workflow run %s has no analysis.", workflowRun.getId()));
          return;
        }

        for (final var output : workflowRun.getAnalysis()) {
          if (output.getExternalKeys().isEmpty()) {
            exchange.setStatusCode(StatusCodes.BAD_REQUEST);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
            exchange
                .getResponseSender()
                .send(
                    String.format(
                        "Workflow run %s has output %s that is not associated with any external"
                            + " identifiers.",
                        workflowRun.getId(), output.getId()));
            return;
          }
          for (final var externalId : output.getExternalKeys()) {
            if (!knownExternalIds.contains(
                new Pair<>(externalId.getProvider(), externalId.getId()))) {
              exchange.setStatusCode(StatusCodes.BAD_REQUEST);
              exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
              exchange
                  .getResponseSender()
                  .send(
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
          if (!output.getType().equals("file") && !output.getType().equals("url")) {
            exchange.setStatusCode(StatusCodes.BAD_REQUEST);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
            exchange
                .getResponseSender()
                .send(
                    String.format(
                        "Workflow run %s has output %s that unknown output type %s.",
                        workflowRun.getId(), output.getId(), output.getType()));
            return;
          }
          if (output.getType().equals("file")
              && (output.getMetatype() == null
                  || output.getMetatype().isBlank()
                  || output.getMd5() == null
                  || output.getMd5().isBlank())) {
            exchange.setStatusCode(StatusCodes.BAD_REQUEST);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
            exchange
                .getResponseSender()
                .send(
                    String.format(
                        "Workflow run %s has file output %s with missing information.",
                        workflowRun.getId(), output.getId()));
            return;
          }
          final var fileDigest = MessageDigest.getInstance("SHA-256");
          fileDigest.update(workflowRun.getId().getBytes(StandardCharsets.UTF_8));
          fileDigest.update(
              (output.getType().equals("file")
                      ? Path.of(output.getPath()).getFileName().toString()
                      : output.getUrl())
                  .getBytes(StandardCharsets.UTF_8));
          final var correctOutputId = hexDigits(fileDigest.digest());
          if (!output.getId().equals(correctOutputId)) {
            exchange.setStatusCode(StatusCodes.BAD_REQUEST);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
            exchange
                .getResponseSender()
                .send(
                    String.format(
                        "Workflow run %s has output %s that should have ID %s.",
                        workflowRun.getId(), output.getId(), correctOutputId));
            return;
          }
        }
      }

      // Okay, if we made it this far, the file is theoretically loadable. There needs to be
      // additional validation against the database, but we will do that in a transaction with the
      // expensive lock.
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
      try (final var connection = dataSource.getConnection()) {
        DSL.using(connection, SQLDialect.POSTGRES)
            .transaction(
                configuration -> loadDataIntoDatabase(unloadedData, workflowInfo, configuration));
        exchange.setStatusCode(StatusCodes.OK);
        exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
        exchange.getResponseSender().send("");
      } catch (IllegalArgumentException e) {
        exchange.setStatusCode(StatusCodes.BAD_REQUEST);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
        exchange.getResponseSender().send(e.getMessage());
      } catch (Exception e) {
        e.printStackTrace();
        exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
        exchange.getResponseSender().send(e.getMessage());
      } finally {
        epochLock.writeLock().unlock();
      }
    } catch (JsonProcessingException | NoSuchAlgorithmException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void loadDataIntoDatabase(
      UnloadedData unloadedData,
      TreeMap<String, Pair<UnloadedWorkflow, Map<String, Pair<String, UnloadedWorkflowVersion>>>>
          workflowInfo,
      Configuration configuration)
      throws JsonProcessingException, NoSuchAlgorithmException {
    final var workflowId = new TreeMap<String, Map<String, Integer>>();
    for (final var info : workflowInfo.values()) {
      final var workflowName = info.first().getName();
      final var workflowLabels = info.first().getLabels();
      var labels =
          Objects.requireNonNullElse(
              upsertWorkflowReturningLabels(configuration, workflowName, workflowLabels), Map.of());
      if (!labels.equals(Objects.requireNonNullElse(workflowLabels, Map.of()))) {
        throw new IllegalArgumentException(
            String.format(
                "Workflow %s has mismatched labels to what is in the database."
                    + " Cannot load this data.",
                workflowName));
      }
      final var workflowVersionIds = new TreeMap<String, Integer>();
      workflowId.put(workflowName, workflowVersionIds);
      for (final var version : info.second().values()) {
        final var workflowScript = version.second().getWorkflow();
        final var rootWorkflowHash =
            hexDigits(
                MessageDigest.getInstance("SHA-256")
                    .digest(workflowScript.getBytes(StandardCharsets.UTF_8)));
        WorkflowLanguage workflowLanguage = version.second().getLanguage();
        insertWorkflowDefinition(configuration, workflowScript, rootWorkflowHash, workflowLanguage);

        final var workflowHashId = version.first();
        final var workflowVersion = version.second().getVersion();
        final var outputs = version.second().getOutputs();
        final var parameters = version.second().getParameters();
        final var id =
            insertWorkflowVersion(
                configuration,
                workflowName,
                rootWorkflowHash,
                workflowHashId,
                workflowVersion,
                outputs,
                parameters);
        // We will have no ID from the insert if it already there
        if (id.isPresent()) {
          workflowVersionIds.put(workflowVersion, id.get().value1());
          for (final var accessory : version.second().getAccessoryFiles().entrySet()) {
            final var accessoryWorkflowHash =
                hexDigits(
                    MessageDigest.getInstance("SHA-256")
                        .digest(accessory.getValue().getBytes(StandardCharsets.UTF_8)));
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
                  .where(WORKFLOW_VERSION.HASH_ID.eq(workflowHashId))
                  .fetchOptional(WORKFLOW_VERSION.ID)
                  .orElseThrow());
        }
      }
    }
    final var now = OffsetDateTime.now();
    for (final var run : unloadedData.getWorkflowRuns()) {
      final var id =
          insertWorkflowRun(
              configuration,
              workflowId.get(run.getWorkflowName()).get(run.getWorkflowVersion()),
              now,
              run);
      if (id.isPresent()) {
        for (final var externalId : run.getExternalKeys()) {

          insertExternalKey(configuration, id.get(), externalId);
        }
        for (final var analysis : run.getAnalysis()) {
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
    final var recoveredWorkflows = new ArrayList<Runnable>();
    processor.recover(recoveredWorkflows::add);
    if (recoveredWorkflows.isEmpty()) {
      System.err.println("No unstarted workflows in the database. Resuming normal operation.");
    } else {
      System.err.printf(
          "Recovering %d unstarted workflows fom the database.", recoveredWorkflows.size());
      recoveredWorkflows.forEach(Runnable::run);
    }
  }

  private void status(HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/html; charset=utf8");
    exchange.setStatusCode(StatusCodes.OK);
    status.renderPage(exchange.getOutputStream());
  }

  private void submit(HttpServerExchange exchange, SubmitWorkflowRequest body) {
    final var postCommitAction = new AtomicReference<Runnable>();
    final var response =
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
            new DatabaseBackedProcessor.SubmissionResultHandler<
                Pair<Integer, SubmitWorkflowResponse>>() {
              @Override
              public boolean allowLaunch() {
                return body.getMode() == SubmitMode.RUN;
              }

              @Override
              public Pair<Integer, SubmitWorkflowResponse> dryRunResult() {
                return new Pair<>(StatusCodes.OK, new SubmitWorkflowResponseDryRun());
              }

              @Override
              public Pair<Integer, SubmitWorkflowResponse> externalIdMismatch() {
                return new Pair<>(
                    StatusCodes.BAD_REQUEST,
                    new SubmitWorkflowResponseFailure("External IDs do not match"));
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
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
      e.printStackTrace();
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
      final var id =
          unloadSearch(
              request,
              (tx, ids) -> {
                final var time = Instant.now();
                final var filename = String.format("unload-%s.json", time);
                try (final var output =
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
                tx.dsl().delete(WORKFLOW_RUN).where(WORKFLOW_RUN.ID.eq(DSL.any(ids))).execute();
                epoch = time.toEpochMilli();
                return filename;
              });
      exchange.setStatusCode(StatusCodes.OK);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_JSON);
      exchange.getResponseSender().send(MAPPER.writeValueAsString(id));
    } catch (Exception e) {
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, CONTENT_TYPE_TEXT);
      exchange.getResponseSender().send(e.getMessage());
      e.printStackTrace();
    } finally {
      epochLock.writeLock().unlock();
    }
  }

  private <T> T unloadSearch(UnloadRequest request, UnloadProcessor<T> handleWorkflowRuns)
      throws SQLException {
    try (var connection = dataSource.getConnection()) {
      return DSL.using(connection, SQLDialect.POSTGRES)
          .transactionResult(
              configuration -> {
                final var workflowRuns =
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
                                                    final var items =
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
                                                    return match(WORKFLOW_RUN.HASH_ID, ids);
                                                  }
                                                })))
                            .fetch(WORKFLOW_RUN.ID));
                if (request.isRecursive()) {
                  Collection<Integer> latestIds = workflowRuns;
                  do {
                    latestIds =
                        DSL.using(configuration)
                            .select(WORKFLOW_RUN.ID)
                            .from(WORKFLOW_RUN)
                            .where(
                                WORKFLOW_RUN
                                    .COMPLETED
                                    .isNotNull()
                                    .and(workflowUsesInputFrom(latestIds)))
                            .fetch(WORKFLOW_RUN.ID);
                    workflowRuns.addAll(latestIds);
                  } while (!latestIds.isEmpty());
                }
                return handleWorkflowRuns.process(
                    configuration, workflowRuns.toArray(Integer[]::new));
              });
    }
  }

  private void updateVersions(HttpServerExchange exchange, BulkVersionRequest request) {
    final var result = processor.updateVersions(request);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, TextFormat.CONTENT_TYPE_004);
    exchange.setStatusCode(StatusCodes.OK);
    try (final var os = exchange.getOutputStream()) {
      os.write(Integer.toString(result).getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private String upsertWorkflowDefinition(
      DSLContext dsl, WorkflowLanguage language, String workflow) throws NoSuchAlgorithmException {
    final var definitionHash =
        hexDigits(
            MessageDigest.getInstance("SHA-256").digest(workflow.getBytes(StandardCharsets.UTF_8)));
    dsl.insertInto(WORKFLOW_DEFINITION)
        .columns(
            WORKFLOW_DEFINITION.HASH_ID,
            WORKFLOW_DEFINITION.WORKFLOW_FILE,
            WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE)
        .values(definitionHash, workflow, language)
        .onConflict(WORKFLOW_DEFINITION.HASH_ID)
        .doNothing()
        .execute();
    return definitionHash;
  }

  private Map<String, BasicType> upsertWorkflowReturningLabels(
      Configuration configuration, String workflowName, Map<String, BasicType> workflowLabels)
      throws JsonProcessingException {
    var result =
        Optional.ofNullable(
                DSL.using(configuration)
                    .insertInto(WORKFLOW)
                    .set(WORKFLOW.NAME, workflowName)
                    .set(WORKFLOW.IS_ACTIVE, false)
                    .set(WORKFLOW.MAX_IN_FLIGHT, 0)
                    .set(WORKFLOW.LABELS, JSONB.valueOf(MAPPER.writeValueAsString(workflowLabels)))
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

  private Condition workflowUsesInputFrom(Collection<Integer> workflowIds) {
    return DSL.exists(
        DSL.select()
            .from(ANALYSIS)
            .where(
                ANALYSIS
                    .WORKFLOW_RUN_ID
                    .eq(DSL.any(workflowIds.toArray(Integer[]::new)))
                    .and(ANALYSIS.HASH_ID.eq(DSL.any(WORKFLOW_RUN.INPUT_FILE_IDS)))));
  }
}
