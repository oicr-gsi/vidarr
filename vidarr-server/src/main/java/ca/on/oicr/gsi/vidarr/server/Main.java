package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.*;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import ca.on.oicr.gsi.status.*;
import ca.on.oicr.gsi.vidarr.*;
import ca.on.oicr.gsi.vidarr.api.AddWorkflowRequest;
import ca.on.oicr.gsi.vidarr.api.AddWorkflowVersionRequest;
import ca.on.oicr.gsi.vidarr.api.AnalysisOutputType;
import ca.on.oicr.gsi.vidarr.api.AnalysisProvenanceRequest;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceAnalysisRecord;
import ca.on.oicr.gsi.vidarr.api.SubmitMode;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowRequest;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponse;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseConflict;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseDryRun;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseFailure;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseMissingKeyVersions;
import ca.on.oicr.gsi.vidarr.api.SubmitWorkflowResponseSuccess;
import ca.on.oicr.gsi.vidarr.api.VersionPolicy;
import ca.on.oicr.gsi.vidarr.core.BaseProcessor;
import ca.on.oicr.gsi.vidarr.core.FileMetadata;
import ca.on.oicr.gsi.vidarr.core.OperationStatus;
import ca.on.oicr.gsi.vidarr.core.Phase;
import ca.on.oicr.gsi.vidarr.core.Target;
import ca.on.oicr.gsi.vidarr.server.DatabaseBackedProcessor.DeleteResultHandler;
import ca.on.oicr.gsi.vidarr.server.dto.*;
import ca.on.oicr.gsi.vidarr.server.remote.RemoteInputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.server.remote.RemoteOutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.server.remote.RemoteRuntimeProvisionerProvider;
import ca.on.oicr.gsi.vidarr.server.remote.RemoteWorkflowEngineProvider;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.ServiceLoader.Provider;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
import org.flywaydb.core.Flyway;
import org.jooq.*;
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
                      e.getResponseSender().send(exception.getMessage());
                    }
                  });
    }

    void handleRequest(HttpServerExchange exchange, T body);
  }

  static final HttpClient CLIENT =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .followRedirects(HttpClient.Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(20))
          .build();
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final JsonFactory MAPPER_FACTORY = new JsonFactory().setCodec(MAPPER);
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
    STATUS_FIELDS.add(DSL.jsonEntry("completed", WORKFLOW_RUN.COMPLETED));
    STATUS_FIELDS.add(
        DSL.jsonEntry(
            "operationStatus",
            DSL.case_(
                    DSL.nvl(
                        DSL.field(
                            DSL.select(
                                    DSL.max(
                                        DSL.case_(ACTIVE_OPERATION.STATUS)
                                            .when(OperationStatus.FAILED, 2)
                                            .when(OperationStatus.SUCCEEDED, 0)
                                            .otherwise(1)))
                                .from(ACTIVE_OPERATION)
                                .where(
                                    ACTIVE_OPERATION
                                        .WORKFLOW_RUN_ID
                                        .eq(WORKFLOW_RUN.ID)
                                        .and(
                                            ACTIVE_OPERATION.ATTEMPT.eq(
                                                ACTIVE_WORKFLOW_RUN.ATTEMPT)))),
                        -1))
                .when(0, "SUCCEEDED")
                .when(2, "FAILED")
                .when(-1, "N/A")
                .otherwise("WAITING")));
    STATUS_FIELDS.add(DSL.jsonEntry("created", WORKFLOW_RUN.CREATED));
    STATUS_FIELDS.add(DSL.jsonEntry("id", WORKFLOW_RUN.HASH_ID));
    STATUS_FIELDS.add(DSL.jsonEntry("inputFiles", WORKFLOW_RUN.INPUT_FILE_IDS));
    STATUS_FIELDS.add(DSL.jsonEntry("labels", WORKFLOW_RUN.LABELS));
    STATUS_FIELDS.add(DSL.jsonEntry("modified", WORKFLOW_RUN.MODIFIED));
    STATUS_FIELDS.add(DSL.jsonEntry("started", WORKFLOW_RUN.STARTED));
    STATUS_FIELDS.add(DSL.jsonEntry("arguments", WORKFLOW_RUN.ARGUMENTS));
    STATUS_FIELDS.add(DSL.jsonEntry("engineParameters", WORKFLOW_RUN.ENGINE_PARAMETERS));
    STATUS_FIELDS.add(DSL.jsonEntry("metadata", WORKFLOW_RUN.METADATA));
    STATUS_FIELDS.add(DSL.jsonEntry("waiting_resource", ACTIVE_WORKFLOW_RUN.WAITING_RESOURCE));
    STATUS_FIELDS.add(
        DSL.jsonEntry(
            "running",
            DSL.nvl(DSL.field(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE.ne(Phase.FAILED)), false)));
    STATUS_FIELDS.add(DSL.jsonEntry("attempt", ACTIVE_WORKFLOW_RUN.ATTEMPT));
    STATUS_FIELDS.add(
        DSL.jsonEntry(
            "enginePhase",
            DSL.case_(ACTIVE_WORKFLOW_RUN.ENGINE_PHASE)
                .mapValues(
                    Stream.of(Phase.values())
                        .collect(Collectors.toMap(Function.identity(), Phase::name)))));

    STATUS_FIELDS.add(DSL.jsonEntry("preflightOk", ACTIVE_WORKFLOW_RUN.PREFLIGHT_OKAY));
    STATUS_FIELDS.add(DSL.jsonEntry("target", ACTIVE_WORKFLOW_RUN.TARGET));
    STATUS_FIELDS.add(DSL.jsonEntry("workflowRunUrl", ACTIVE_WORKFLOW_RUN.WORKFLOW_RUN_URL));
    STATUS_FIELDS.add(
        DSL.jsonEntry(
            "operations",
            DSL.field(
                DSL.select(
                        DSL.jsonArrayAgg(
                            DSL.jsonObject(
                                DSL.jsonEntry("attempt", ACTIVE_OPERATION.ATTEMPT),
                                DSL.jsonEntry("recoveryState", ACTIVE_OPERATION.RECOVERY_STATE),
                                DSL.jsonEntry("debugInformation", ACTIVE_OPERATION.DEBUG_INFO),
                                DSL.jsonEntry("status", ACTIVE_OPERATION.STATUS),
                                DSL.jsonEntry("type", ACTIVE_OPERATION.TYPE))))
                    .from(ACTIVE_OPERATION)
                    .where(ACTIVE_OPERATION.WORKFLOW_RUN_ID.eq(WORKFLOW_RUN.ID)))));
  }

  private static Field<?> createQuery(VersionPolicy policy, Set<String> allowedTypes) {
    switch (policy) {
      case ALL:
        {
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
                                  DSL.select(DSL.jsonArrayAgg(externalIdVersionAlias.VALUE))
                                      .from(externalIdVersionAlias)
                                      .where(
                                          externalIdVersionAlias
                                              .KEY
                                              .eq(table.field(0, String.class))
                                              .and(
                                                  externalIdVersionAlias.EXTERNAL_ID_ID.eq(
                                                      EXTERNAL_ID.ID)))))))
                  .from(table));
        }
      case LATEST:
        {
          var condition = EXTERNAL_ID.ID.eq(EXTERNAL_ID_VERSION.EXTERNAL_ID_ID);
          if (allowedTypes != null) {
            condition = condition.and(EXTERNAL_ID_VERSION.KEY.in(allowedTypes));
          }
          final var externalIdVersionAlias = EXTERNAL_ID_VERSION.as("externalIdVersionInner");
          return DSL.field(
              DSL.select(
                      DSL.jsonObjectAgg(
                          DSL.jsonEntry(
                              EXTERNAL_ID_VERSION.KEY,
                              DSL.field(
                                  DSL.select(
                                          DSL.lastValue(externalIdVersionAlias.VALUE)
                                              .over()
                                              .orderBy(externalIdVersionAlias.CREATED))
                                      .from(externalIdVersionAlias)
                                      .where(
                                          externalIdVersionAlias
                                              .KEY
                                              .eq(EXTERNAL_ID_VERSION.KEY)
                                              .and(
                                                  externalIdVersionAlias.EXTERNAL_ID_ID.eq(
                                                      EXTERNAL_ID.ID)))))))
                  .from(EXTERNAL_ID_VERSION)
                  .where(condition)
                  .groupBy(EXTERNAL_ID_VERSION.KEY));
        }
      default:
        return DSL.inline(null, SQLDataType.JSON);
    }
  }

  private static void handleException(HttpServerExchange exchange) {
    final var e = (Exception) exchange.getAttachment(ExceptionHandler.THROWABLE);
    exchange.setStatusCode(500);
    e.printStackTrace();
    exchange.getResponseSender().send(e.getMessage());
  }

  @SafeVarargs
  private static <P, T> Map<String, T> load(
      String name,
      Class<P> clazz,
      Function<P, String> namer,
      BiFunction<P, ObjectNode, T> reader,
      Map<String, ObjectNode> configuration,
      P... fixedProviders) {
    final var providers =
        Stream.concat(
                ServiceLoader.load(clazz).stream().map(Provider::get), Stream.of(fixedProviders))
            .collect(Collectors.toMap(namer, Function.identity()));
    final var output = new TreeMap<String, T>();
    for (final var entry : configuration.entrySet()) {
      if (!entry.getValue().has("type")) {
        throw new IllegalArgumentException(
            String.format("%s record %s lacks type", name, entry.getKey()));
      }
      final var type = entry.getValue().get("type").asText();
      final var provider = providers.get(type);
      if (provider == null) {
        throw new IllegalArgumentException(
            String.format(
                "%s record %s has type %s, but this is not registered. Maybe a missing module?",
                name, entry.getKey(), type));
      }
      output.put(entry.getKey(), reader.apply(provider, entry.getValue()));
    }
    return output;
  }

  private static Map<String, ConsumableResource> loadConsumableResources(
      Map<String, ObjectNode> configuration) {
    final var providers =
        ServiceLoader.load(ConsumableResourceProvider.class).stream()
            .map(Provider::get)
            .collect(Collectors.toMap(ConsumableResourceProvider::type, Function.identity()));
    final var output = new TreeMap<String, ConsumableResource>();
    for (final var entry : configuration.entrySet()) {
      if (!entry.getValue().has("type")) {
        throw new IllegalArgumentException(
            String.format("Consumable resource record %s lacks type", entry.getKey()));
      }
      final var type = entry.getValue().get("type").asText();
      final var provider = providers.get(type);
      if (provider == null) {
        throw new IllegalArgumentException(
            String.format(
                "Consumable resource record %s has type %s, but this is not registered. Maybe a"
                    + " missing module?",
                entry.getKey(), type));
      }
      output.put(entry.getKey(), provider.readConfiguration(entry.getKey(), entry.getValue()));
    }
    return output;
  }

  public static void main(String[] args) throws IOException, SQLException {
    if (args.length != 1) {
      System.err.println(
          "Usage: java --module-path MODULES --module ca.on.oicr.gsi.vidarr.server"
              + " configuration.json");
    }
    DefaultExports.initialize();
    final var server = new Main(MAPPER.readValue(new File(args[0]), ServerConfiguration.class));
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
                            .get("/api/workflows", monitor(server::fetchWorkflows))
                            .get("/metrics", monitor(new BlockingHandler(Main::metrics)))
                            .post(
                                "/api/provenance",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(
                                            AnalysisProvenanceRequest.class,
                                            server::fetchProvenance))))
                            .delete(
                                "/api/status/{hash}",
                                monitor(new BlockingHandler(server::deleteWorkflowRun)))
                            .post(
                                "/api/submit",
                                monitor(
                                    JsonPost.parse(SubmitWorkflowRequest.class, server::submit)))
                            .post(
                                "/api/workflow/{name}",
                                monitor(
                                    new BlockingHandler(
                                        JsonPost.parse(
                                            AddWorkflowRequest.class, server::addWorkflow))))
                            .delete(
                                "/api/workflow/{name}",
                                monitor(new BlockingHandler(server::disableWorkflow)))
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

  private Main(ServerConfiguration configuration) throws SQLException {
    selfUrl = configuration.getUrl();
    selfName = configuration.getName();
    port = configuration.getPort();
    otherServers = configuration.getOtherServers();
    workflowEngines =
        load(
            "Workflow engine",
            WorkflowEngineProvider.class,
            WorkflowEngineProvider::type,
            WorkflowEngineProvider::readConfiguration,
            configuration.getWorkflowEngines(),
            new RemoteWorkflowEngineProvider());
    inputProvisioners =
        load(
            "Input Provisioner",
            InputProvisionerProvider.class,
            InputProvisionerProvider::type,
            InputProvisionerProvider::readConfiguration,
            configuration.getInputProvisioners(),
            new RemoteInputProvisionerProvider());
    outputProvisioners =
        load(
            "Output Provisioner",
            OutputProvisionerProvider.class,
            OutputProvisionerProvider::type,
            OutputProvisionerProvider::readConfiguration,
            configuration.getOutputProvisioners(),
            new RemoteOutputProvisionerProvider());
    runtimeProvisioners =
        load(
            "Runtime Provisioner",
            RuntimeProvisionerProvider.class,
            RuntimeProvisionerProvider::name,
            RuntimeProvisionerProvider::readConfiguration,
            configuration.getRuntimeProvisioners(),
            new RemoteRuntimeProvisionerProvider());
    final var consumableResources = loadConsumableResources(configuration.getConsumableResources());
    targets =
        configuration.getTargets().entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        new Target() {
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
                          private final List<ConsumableResource> consumables =
                              Stream.concat(
                                      e.getValue().getConsumableResources().stream()
                                          .map(consumableResources::get),
                                      Stream.of(maxInFlightPerWorkflow))
                                  .collect(Collectors.toList());

                          @Override
                          public Stream<ConsumableResource> consumableResources() {
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
    Flyway.configure().dataSource(simpleConnection).load().migrate();

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
                      private final String path = result.getFilePath();

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
  }

  private void addWorkflow(HttpServerExchange exchange, AddWorkflowRequest request) {
    final var name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");

    try (final var connection = dataSource.getConnection()) {
      final var dsl = DSL.using(connection, SQLDialect.POSTGRES);
      dsl.insertInto(WORKFLOW)
          .columns(WORKFLOW.NAME, WORKFLOW.LABELS, WORKFLOW.IS_ACTIVE, WORKFLOW.MAX_IN_FLIGHT)
          .values(
              name,
              JSONB.valueOf(MAPPER.writeValueAsString(request.getLabels())),
              true,
              request.getMaxInFlight())
          .onConflict(WORKFLOW.NAME)
          .doUpdate()
          .set(WORKFLOW.IS_ACTIVE, true)
          .set(WORKFLOW.MAX_IN_FLIGHT, request.getMaxInFlight())
          .execute();
      connection.commit();
      maxInFlightPerWorkflow.set(name, request.getMaxInFlight());
      exchange.setStatusCode(StatusCodes.OK);
      exchange.getResponseSender().send("");
    } catch (SQLException | JsonProcessingException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void addWorkflowVersion(HttpServerExchange exchange, AddWorkflowVersionRequest request) {
    final var name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");
    final var version =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("version");
    try (final var connection = dataSource.getConnection()) {
      final var dsl = DSL.using(connection, SQLDialect.POSTGRES);
      final var definitionHash =
          BaseProcessor.hexDigits(
              MessageDigest.getInstance("SHA-256")
                  .digest(request.getWorkflow().getBytes(StandardCharsets.UTF_8)));
      final var definitionId =
          dsl.insertInto(WORKFLOW_DEFINITION)
              .columns(
                  WORKFLOW_DEFINITION.HASH_ID,
                  WORKFLOW_DEFINITION.WORKFLOW_FILE,
                  WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE)
              .values(definitionHash, request.getWorkflow(), request.getLanguage())
              .onDuplicateKeyIgnore()
              .returningResult(WORKFLOW_DEFINITION.ID)
              .fetchOptional()
              .orElseThrow()
              .value1();

      final var versionDigest = MessageDigest.getInstance("SHA-256");
      versionDigest.update(name.getBytes(StandardCharsets.UTF_8));
      versionDigest.update(new byte[] {0});
      versionDigest.update(version.getBytes(StandardCharsets.UTF_8));
      versionDigest.update(new byte[] {0});
      versionDigest.update(definitionHash.getBytes(StandardCharsets.UTF_8));
      versionDigest.update(MAPPER.writeValueAsBytes(request.getOutputs()));
      versionDigest.update(MAPPER.writeValueAsBytes(request.getParameters()));

      final var accessoryIds = new TreeMap<String, Integer>();
      for (final var accessory : new TreeMap<>(request.getAccessoryFiles()).entrySet()) {
        final var accessoryHash =
            BaseProcessor.hexDigits(
                MessageDigest.getInstance("SHA-256")
                    .digest(accessory.getValue().getBytes(StandardCharsets.UTF_8)));
        versionDigest.update(new byte[] {0});
        versionDigest.update(accessory.getKey().getBytes(StandardCharsets.UTF_8));
        versionDigest.update(new byte[] {0});
        versionDigest.update(accessoryHash.getBytes(StandardCharsets.UTF_8));
        accessoryIds.put(
            accessory.getKey(),
            dsl.insertInto(WORKFLOW_DEFINITION)
                .columns(
                    WORKFLOW_DEFINITION.HASH_ID,
                    WORKFLOW_DEFINITION.WORKFLOW_FILE,
                    WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE)
                .values(accessoryHash, accessory.getValue(), request.getLanguage())
                .onDuplicateKeyIgnore()
                .returningResult(WORKFLOW_DEFINITION.ID)
                .fetchOptional()
                .orElseThrow()
                .value1());
      }
      final var versionHash = BaseProcessor.hexDigits(versionDigest.digest());
      dsl.select(DSL.field(WORKFLOW_VERSION.HASH_ID.eq(versionHash)))
          .from(WORKFLOW_VERSION)
          .where(WORKFLOW_VERSION.NAME.eq(name).and(WORKFLOW_VERSION.VERSION.eq(version)))
          .fetchOptional()
          .ifPresentOrElse(
              record -> {
                if (record.value1()) {
                  dsl.update(WORKFLOW)
                      .set(WORKFLOW.IS_ACTIVE, true)
                      .where(WORKFLOW.NAME.eq(name))
                      .execute();
                  exchange.setStatusCode(StatusCodes.OK);
                } else {
                  exchange.setStatusCode(StatusCodes.CONFLICT);
                }
                exchange.getResponseSender().send("");
              },
              () -> {
                final var id =
                    dsl.insertInto(
                            WORKFLOW_VERSION,
                            WORKFLOW_VERSION.HASH_ID,
                            WORKFLOW_VERSION.METADATA,
                            WORKFLOW_VERSION.NAME,
                            WORKFLOW_VERSION.PARAMETERS,
                            WORKFLOW_VERSION.WORKFLOW_DEFINITION,
                            WORKFLOW_VERSION.VERSION)
                        .values(
                            versionHash,
                            MAPPER.valueToTree(request.getOutputs()),
                            name,
                            MAPPER.valueToTree(request.getParameters()),
                            definitionId,
                            version)
                        .returningResult(WORKFLOW_VERSION.ID)
                        .fetchOne();
                if (id == null) {
                  exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
                  exchange.getResponseSender().send("Failed to insert");
                } else {
                  if (!accessoryIds.isEmpty()) {

                    var accessoryQuery =
                        dsl.insertInto(
                            WORKFLOW_VERSION_ACCESSORY,
                            WORKFLOW_VERSION_ACCESSORY.WORKFLOW_VERSION,
                            WORKFLOW_VERSION_ACCESSORY.FILENAME,
                            WORKFLOW_VERSION_ACCESSORY.WORKFLOW_DEFINITION);

                    for (final var accessory : accessoryIds.entrySet()) {
                      accessoryQuery =
                          accessoryQuery.values(
                              id.value1(), accessory.getKey(), accessory.getValue());
                    }
                    accessoryQuery.execute();
                  }
                  exchange.setStatusCode(StatusCodes.OK);
                  exchange.getResponseSender().send("");
                }
              });
      connection.commit();
    } catch (SQLException | NoSuchAlgorithmException | JsonProcessingException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private Field<JSON> createAnalysisJsonField(Field<JSON> externalKeys, JSONEntry<?>... extra) {
    final var analysisCommonFields = new ArrayList<>(List.of(extra));
    analysisCommonFields.add(DSL.jsonEntry("id", ANALYSIS.HASH_ID));
    analysisCommonFields.add(DSL.jsonEntry("created", ANALYSIS.CREATED));
    analysisCommonFields.add(DSL.jsonEntry("labels", ANALYSIS.LABELS));
    analysisCommonFields.add(DSL.jsonEntry("modified", ANALYSIS.MODIFIED));
    analysisCommonFields.add(DSL.jsonEntry("externalKeys", externalKeys));

    final var analysisFileFields = new ArrayList<>(analysisCommonFields);
    analysisFileFields.add(DSL.jsonEntry("path", ANALYSIS.FILE_PATH));
    analysisFileFields.add(DSL.jsonEntry("md5", ANALYSIS.FILE_MD5SUM));
    analysisFileFields.add(DSL.jsonEntry("metatype", ANALYSIS.FILE_METATYPE));
    analysisFileFields.add(DSL.jsonEntry("size", ANALYSIS.FILE_SIZE));

    final var analysisUrlFields = new ArrayList<>(analysisCommonFields);
    analysisUrlFields.add(DSL.jsonEntry("url", ANALYSIS.FILE_PATH));

    return DSL.case_(ANALYSIS.ANALYSIS_TYPE)
        .when("file", DSL.jsonObject(analysisFileFields))
        .when("url", DSL.jsonObject(analysisUrlFields));
  }

  private void createAnalysisRecords(
      Connection connection,
      JsonGenerator jsonGenerator,
      VersionPolicy policy,
      Set<String> allowedTypes,
      boolean includeParameters,
      Set<AnalysisOutputType> includedAnalyses,
      Condition condition)
      throws SQLException {
    final var fields = new ArrayList<JSONEntry<?>>();

    fields.add(DSL.jsonEntry("completed", WORKFLOW_RUN.COMPLETED));
    fields.add(DSL.jsonEntry("created", WORKFLOW_RUN.CREATED));
    fields.add(DSL.jsonEntry("id", WORKFLOW_RUN.HASH_ID));
    fields.add(DSL.jsonEntry("inputFiles", WORKFLOW_RUN.INPUT_FILE_IDS));
    fields.add(DSL.jsonEntry("labels", WORKFLOW_RUN.LABELS));
    fields.add(DSL.jsonEntry("modified", WORKFLOW_RUN.MODIFIED));
    fields.add(DSL.jsonEntry("started", WORKFLOW_RUN.STARTED));

    if (includeParameters) {
      fields.add(DSL.jsonEntry("arguments", WORKFLOW_RUN.ARGUMENTS));
      fields.add(DSL.jsonEntry("engineParameters", WORKFLOW_RUN.ENGINE_PARAMETERS));
      fields.add(DSL.jsonEntry("metadata", WORKFLOW_RUN.METADATA));
    }
    fields.add(
        DSL.jsonEntry(
            "workflowName",
            DSL.field(
                DSL.select(WORKFLOW_VERSION.NAME)
                    .from(WORKFLOW_VERSION)
                    .where(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID)))));
    fields.add(
        DSL.jsonEntry(
            "workflowVersion",
            DSL.field(
                DSL.select(WORKFLOW_VERSION.VERSION)
                    .from(WORKFLOW_VERSION)
                    .where(WORKFLOW_RUN.WORKFLOW_VERSION_ID.eq(WORKFLOW_VERSION.ID)))));

    fields.add(
        DSL.jsonEntry(
            "externalKeys",
            DSL.field(
                DSL.select(
                        DSL.jsonArrayAgg(
                            DSL.jsonObject(
                                DSL.jsonEntry("id", EXTERNAL_ID.EXTERNAL_ID_),
                                DSL.jsonEntry("provider", EXTERNAL_ID.PROVIDER),
                                DSL.jsonEntry("created", EXTERNAL_ID.CREATED),
                                DSL.jsonEntry("modified", EXTERNAL_ID.MODIFIED),
                                DSL.jsonEntry("requested", EXTERNAL_ID.REQUESTED),
                                DSL.jsonEntry("versions", createQuery(policy, allowedTypes)))))
                    .from(EXTERNAL_ID)
                    .where(EXTERNAL_ID.WORKFLOW_RUN_ID.eq(WORKFLOW_RUN.ID)))));

    fields.add(
        DSL.jsonEntry(
            "analysis",
            DSL.field(
                DSL.select(
                        DSL.jsonArrayAgg(
                            createAnalysisJsonField(
                                DSL.field(
                                    DSL.select(
                                            DSL.jsonArrayAgg(
                                                DSL.jsonObject(
                                                    DSL.jsonEntry("provider", EXTERNAL_ID.PROVIDER),
                                                    DSL.jsonEntry("id", EXTERNAL_ID.EXTERNAL_ID_))))
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
    DSL.using(connection, SQLDialect.POSTGRES)
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
    exchange.getResponseSender().send("");
  }

  private void disableWorkflow(HttpServerExchange exchange) {
    final var name =
        exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");

    try (final var connection = dataSource.getConnection()) {
      final var dsl = DSL.using(connection, SQLDialect.POSTGRES);
      final var count =
          dsl.update(WORKFLOW)
              .set(WORKFLOW.IS_ACTIVE, false)
              .where(WORKFLOW.NAME.eq(name))
              .execute();
      connection.commit();
      exchange.setStatusCode(count == 0 ? StatusCodes.NOT_FOUND : StatusCodes.OK);
      exchange.getResponseSender().send("");
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchAllActive(HttpServerExchange exchange) {

    try (final var connection = dataSource.getConnection();
        final var output = MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
      exchange.setStatusCode(StatusCodes.OK);
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
                                      DSL.jsonEntry("id", EXTERNAL_ID.EXTERNAL_ID_),
                                      DSL.jsonEntry("provider", EXTERNAL_ID.PROVIDER),
                                      DSL.jsonEntry("created", EXTERNAL_ID.CREATED),
                                      DSL.jsonEntry("modified", EXTERNAL_ID.MODIFIED),
                                      DSL.jsonEntry("requested", EXTERNAL_ID.REQUESTED),
                                      DSL.jsonEntry(
                                          "versions", createQuery(VersionPolicy.ALL, null)))))
                          .from(
                              EXTERNAL_ID
                                  .join(ANALYSIS_EXTERNAL_ID)
                                  .on(EXTERNAL_ID.ID.eq(ANALYSIS_EXTERNAL_ID.EXTERNAL_ID_ID)))
                          .where(ANALYSIS_EXTERNAL_ID.ANALYSIS_ID.eq(ANALYSIS.ID))),
                  DSL.jsonEntry(
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
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.setStatusCode(StatusCodes.OK);
                exchange.getResponseSender().send(result.data());
              },
              () -> {
                exchange.setStatusCode(StatusCodes.NOT_FOUND);
                exchange.getResponseSender().send("");
              });
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchFile(HttpServerExchange exchange) {
    fetchAnalysis(exchange, "file");
  }

  private void fetchProvenance(HttpServerExchange exchange, AnalysisProvenanceRequest request) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
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
            connection,
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
      exchange.getResponseSender().send(e.getMessage());
    } finally {
      epochLock.readLock().unlock();
    }
  }

  private void fetchRun(HttpServerExchange exchange) {
    try (final var connection = dataSource.getConnection()) {
      final var vidarrId =
          exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("hash");
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
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
                exchange.setStatusCode(complete ? StatusCodes.OK : StatusCodes.PARTIAL_CONTENT);
                try (final var output =
                    MAPPER_FACTORY.createGenerator(exchange.getOutputStream())) {
                  createAnalysisRecords(
                      connection,
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
                exchange.getResponseSender().send("");
              });
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
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
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.setStatusCode(StatusCodes.OK);
                exchange.getResponseSender().send(record.data());
              },
              () -> {
                exchange.setStatusCode(StatusCodes.NOT_FOUND);
                exchange.getResponseSender().send("null");
              });
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  private void fetchTargets(HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
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
          .flatMap(cr -> cr.inputFromUser().stream())
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

  private void fetchWorkflows(HttpServerExchange exchange) {
    try (final var connection = dataSource.getConnection()) {
      final var workflowVersionAlias = WORKFLOW_VERSION.as("other_workflow_version");
      final var response =
          DSL.using(connection, SQLDialect.POSTGRES)
              .select(
                  DSL.jsonArrayAgg(
                      DSL.jsonObject(
                          DSL.jsonEntry("name", WORKFLOW_VERSION.NAME),
                          DSL.jsonEntry("version", WORKFLOW_VERSION.VERSION),
                          DSL.jsonEntry("metadata", WORKFLOW_VERSION.METADATA),
                          DSL.jsonEntry("parameters", WORKFLOW_VERSION.PARAMETERS),
                          DSL.jsonEntry(
                              "labels",
                              DSL.field(
                                  DSL.select(WORKFLOW.LABELS)
                                      .from(WORKFLOW)
                                      .where(WORKFLOW.NAME.eq(WORKFLOW_VERSION.NAME)))),
                          DSL.jsonEntry("language", WORKFLOW_DEFINITION.WORKFLOW_LANGUAGE))))
              .from(
                  WORKFLOW_VERSION
                      .join(WORKFLOW_DEFINITION)
                      .on(WORKFLOW_VERSION.WORKFLOW_DEFINITION.eq(WORKFLOW_DEFINITION.ID)))
              .where(
                  WORKFLOW_VERSION
                      .NAME
                      .in(DSL.select(WORKFLOW.NAME).from(WORKFLOW).where(WORKFLOW.IS_ACTIVE))
                      .and(
                          WORKFLOW_VERSION.MODIFIED.eq(
                              DSL.select(DSL.max(workflowVersionAlias.MODIFIED))
                                  .from(workflowVersionAlias)
                                  .where(workflowVersionAlias.NAME.eq(WORKFLOW_VERSION.NAME)))))
              .fetchOptional()
              .orElseThrow()
              .value1();
      exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
      exchange.setStatusCode(StatusCodes.OK);
      exchange.getResponseSender().send(response.data());
    } catch (SQLException e) {
      e.printStackTrace();
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send(e.getMessage());
    }
  }

  @Override
  public Stream<Header> headers() {
    return Stream.empty();
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
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
    exchange.setStatusCode(response.first());
    if (postCommitAction.get() != null) {
      postCommitAction.get().run();
    }
    try {
      exchange.getResponseSender().send(MAPPER.writeValueAsString(response.second()));
    } catch (JsonProcessingException e) {
      exchange.setStatusCode(StatusCodes.INTERNAL_SERVER_ERROR);
      exchange.getResponseSender().send(e.getMessage());
      e.printStackTrace();
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
}
