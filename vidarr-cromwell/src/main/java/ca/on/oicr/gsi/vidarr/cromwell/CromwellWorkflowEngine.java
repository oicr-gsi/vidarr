package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prometheus.client.Counter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.xml.stream.XMLStreamException;

/** Run workflows using Cromwell */
public final class CromwellWorkflowEngine
    extends BaseJsonWorkflowEngine<EngineState, String, Void> {
  private static final int CHECK_DELAY = 1;
  static final HttpClient CLIENT =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .followRedirects(HttpClient.Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(20))
          .build();
  static final Counter CROMWELL_FAILURES =
      Counter.build(
              "vidarr_cromwell_failed_requests",
              "The number of failed HTTP requests to the Cromwell server")
          .labelNames("target")
          .register();
  static final Counter CROMWELL_REQUESTS =
      Counter.build(
              "vidarr_cromwell_total_requests",
              "The number of HTTP requests to the Cromwell server")
          .labelNames("target")
          .register();
  static final ObjectMapper MAPPER = new ObjectMapper();

  private static Stream<Path> findAllParents(String file) {
    final var parents = new ArrayList<Path>();
    for (var path = Path.of(file).getParent(); path != null; path = path.getParent()) {
      parents.add(path);
    }
    return parents.stream();
  }

  public static WorkflowEngineProvider provider() {
    return () -> Stream.of(new Pair<>("cromwell", CromwellWorkflowEngine.class));
  }

  static WorkMonitor.Status statusFromCromwell(String status) {
    return switch (status) {
      case "On Hold" -> WorkMonitor.Status.WAITING;
      case "Submitted" -> WorkMonitor.Status.QUEUED;
      case "Running" -> WorkMonitor.Status.RUNNING;
      default -> WorkMonitor.Status.UNKNOWN;
    };
  }

  private Map<String, BasicType> engineParameters;
  private String url;

  // TODO Optimally, this would be Optional<Boolean>
  private Boolean debugInflightRuns;

  public CromwellWorkflowEngine() {
    super(MAPPER, EngineState.class, String.class, Void.class);
  }

  private void check(EngineState state, WorkMonitor<Result<String>, EngineState> monitor) {
    try {
      monitor.log(
          System.Logger.Level.INFO,
          String.format("Checking Cromwell workflow %s on %s", state.getCromwellId(), url));
      CROMWELL_REQUESTS.labels(url).inc();
      CLIENT
          .sendAsync(
              HttpRequest.newBuilder()
                  .uri(
                      CromwellMetadataURL.formatMetadataURL(
                          url, state.getCromwellId(), debugInflightRuns))
                  .timeout(Duration.ofMinutes(1))
                  .GET()
                  .build(),
              new JsonBodyHandler<>(MAPPER, WorkflowMetadataResponse.class))
          .thenApply(HttpResponse::body)
          .thenAccept(
              s -> {
                final var result = s.get();
                monitor.log(
                    System.Logger.Level.INFO,
                    String.format(
                        "Status for Cromwell workflow %s on %s is %s",
                        state.getCromwellId(), url, result.getStatus()));
                monitor.storeDebugInfo(result.debugInfo());
                switch (result.getStatus()) {
                    // In the case of failures ("Aborted" or "Failed"), request the full metadata
                    // from Cromwell if we don't already have it
                    // so we can have call info for debugging.
                  case "Aborted":
                  case "Failed":
                    if (debugInflightRuns) {
                      monitor.log(
                          System.Logger.Level.INFO,
                          String.format("Cromwell job %s is failed. Cromwell WorkflowEngine is "
                              + "configured to have already fetched calls info. Skipping second "
                              + "request.", state.getCromwellId())
                      );
                      monitor.permanentFailure("Cromwell failure: " + result.getStatus());
                      break;
                    }
                    monitor.log(
                        System.Logger.Level.INFO,
                        String.format(
                            "Cromwell job %s is failed, fetching call info on %s",
                            state.getCromwellId(), url));
                    CROMWELL_REQUESTS.labels(url).inc();

                    CLIENT
                        .sendAsync(
                            HttpRequest.newBuilder()
                                .uri(
                                    CromwellMetadataURL.formatMetadataURL(
                                        url, state.getCromwellId(), true))
                                .timeout(Duration.ofMinutes(1))
                                .GET()
                                .build(),
                            new JsonBodyHandler<>(MAPPER, WorkflowMetadataResponse.class))
                        .thenApply(HttpResponse::body)
                        .thenAccept(
                            s2 -> {
                              final var fullResult = s2.get();
                              monitor.log(
                                  System.Logger.Level.INFO,
                                  String.format(
                                      "Successfully fetched full metadata for Cromwell job %s on %s",
                                      state.getCromwellId(), url));
                              monitor.storeDebugInfo(fullResult.debugInfo());
                              monitor.permanentFailure("Cromwell failure: " + result.getStatus());
                            })
                        .exceptionally(
                            t2 -> {
                              t2.printStackTrace();
                              monitor.log(
                                  System.Logger.Level.WARNING,
                                  String.format(
                                      "Failed to get Cromwell job %s on %s due to %s",
                                      state.getCromwellId(), url, t2.getMessage()));
                              CROMWELL_FAILURES.labels(url).inc();

                              // TODO: this schedules 2 requests to cromwell /metadata now. Consider
                              // a failure-unique check
                              monitor.scheduleTask(
                                  CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
                              return null;
                            });
                    break;
                  case "Succeeded":
                    finish(state, monitor);
                    break;
                  default:
                    monitor.updateState(statusFromCromwell(result.getStatus()));
                    monitor.scheduleTask(5, TimeUnit.MINUTES, () -> check(state, monitor));
                }
              })
          .exceptionally(
              t -> {
                t.printStackTrace();
                monitor.log(
                    System.Logger.Level.WARNING,
                    String.format(
                        "Failed to get status for Cromwell workflow %s on %s due to %s",
                        state.getCromwellId(), url, t.getMessage()));
                CROMWELL_FAILURES.labels(url).inc();
                monitor.scheduleTask(5, TimeUnit.MINUTES, () -> check(state, monitor));
                return null;
              });
    } catch (Exception e) {
      e.printStackTrace();
      monitor.log(
          System.Logger.Level.WARNING,
          String.format(
              "Failed to get status for Cromwell workflow %s on %s due to %s",
              state.getCromwellId(), url, e.getMessage()));
      CROMWELL_FAILURES.labels(url).inc();
      monitor.scheduleTask(5, TimeUnit.MINUTES, () -> check(state, monitor));
    }
  }

  @Override
  protected Void cleanup(String cleanupState, WorkMonitor<Void, Void> monitor) {
    monitor.scheduleTask(() -> monitor.complete(null));
    return null;
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    sectionRenderer.link("Server", url, url);
  }

  @Override
  public Optional<BasicType> engineParameters() {
    return Optional.ofNullable(engineParameters)
        .map(
            fields ->
                BasicType.object(
                    fields.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue()))));
  }

  private void finish(EngineState state, WorkMonitor<Result<String>, EngineState> monitor) {
    CROMWELL_REQUESTS.labels(url).inc();
    monitor.log(
        System.Logger.Level.INFO,
        String.format("Reaping output of Cromwell workflow %s on %s", state.getCromwellId(), url));
    CLIENT
        .sendAsync(
            HttpRequest.newBuilder()
                .uri(
                    URI.create(
                        String.format(
                            "%s/api/workflows/v1/%s/outputs", url, state.getCromwellId())))
                .timeout(Duration.ofMinutes(1))
                .GET()
                .build(),
            new JsonBodyHandler<>(MAPPER, WorkflowOutputResponse.class))
        .thenApply(HttpResponse::body)
        .thenAccept(
            s -> {
              final var result = s.get();
              monitor.log(
                  System.Logger.Level.INFO,
                  String.format(
                      "Got output of Cromwell workflow %s on %s", state.getCromwellId(), url));
              monitor.complete(
                  new Result<>(
                      result.getOutputs(),
                      // Note: This instance of the cromwell URL is for use by OutputProvisioners
                      // Don't include 'excludeKeys', includeCalls needs to be true
                      CromwellMetadataURL.formatMetadataURL(url, state.getCromwellId(), true)
                          .toString(),
                      Optional.empty()));
            })
        .exceptionally(
            t -> {
              t.printStackTrace();
              monitor.log(
                  System.Logger.Level.INFO,
                  String.format(
                      "Failed to get output of Cromwell workflow %s on %s",
                      state.getCromwellId(), url));
              CROMWELL_FAILURES.labels(url).inc();
              monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
              return null;
            });
  }

  public String getUrl() {
    return url;
  }

  @Override
  protected void recover(EngineState state, WorkMonitor<Result<String>, EngineState> monitor) {
    if (state.getCromwellId() == null) {
      monitor.scheduleTask(() -> startTask(state, monitor));
    } else {
      check(state, monitor);
    }
  }

  @Override
  protected void recoverCleanup(Void state, WorkMonitor<Void, Void> monitor) {
    monitor.complete(null);
  }

  @Override
  protected EngineState runWorkflow(
      WorkflowLanguage workflowLanguage,
      String workflow,
      Stream<Pair<String, String>> accessoryFiles,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters,
      WorkMonitor<Result<String>, EngineState> monitor) {
    final var state = new EngineState();
    /* Cromwell and Shesmu/Vidarr handle optional parameters differently.
     * Shesmu/Vidarr encoding missing values as "null", but Cromwell encodes
     * them as absent. If a value is "null", Cromwell will complain that it
     * can't be parsed rather than drop the null and use its default.
     * Therefore, we delete all the nulls so we have something Cromwell will
     * accept. There's no situation where Cromwell would _need_ to see a null.
     */
    final var filteredParameters = MAPPER.createObjectNode();
    final var iterator = workflowParameters.fields();
    while (iterator.hasNext()) {
      final var field = iterator.next();
      if (!field.getValue().isNull()) {
        filteredParameters.set(field.getKey(), field.getValue());
      }
    }
    state.setWorkflowInputFiles(
        accessoryFiles.collect(Collectors.toMap(Pair::first, Pair::second)));
    state.setEngineParameters(engineParameters);
    state.setParameters(filteredParameters);
    state.setVidarrId(vidarrId);
    state.setWorkflowLanguage(workflowLanguage);
    state.setWorkflowSource(workflow);
    monitor.scheduleTask(() -> startTask(state, monitor));
    return state;
  }

  public void setEngineParameters(Map<String, BasicType> engineParameters) {
    this.engineParameters = engineParameters;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  private void startTask(EngineState state, WorkMonitor<Result<String>, EngineState> monitor) {
    try {
      monitor.log(System.Logger.Level.INFO, String.format("Starting Cromwell workflow on %s", url));
      final var body =
          new MultiPartBodyPublisher()
              .addPart("workflowSource", state.getWorkflowSource())
              .addPart("workflowInputs", MAPPER.writeValueAsString(state.getParameters()))
              .addPart("workflowType", "WDL")
              .addPart(
                  "workflowTypeVersion",
                  switch (state.getWorkflowLanguage()) {
                    case WDL_1_0 -> "1.0";
                    case WDL_1_1 -> "1.1";
                    default -> "draft1";
                  })
              .addPart(
                  "labels",
                  MAPPER.writeValueAsString(
                      Collections.singletonMap(
                          "vidarr-id",
                          state
                              .getVidarrId()
                              .substring(Math.max(0, state.getVidarrId().length() - 255)))))
              .addPart("workflowOptions", MAPPER.writeValueAsString(state.getEngineParameters()));
      if (!state.getWorkflowInputFiles().isEmpty()) {
        // Cromwell doesn't deduplicate these and stores them all in its database, so it doesn't
        // matter if we make the effort to ensure these ZIP files are byte-for-byte identical.
        final var zipOutput = new ByteArrayOutputStream();
        try (final var zipFile = new ZipOutputStream(zipOutput)) {

          // We have to create all the parent directories or the better-files compressor that
          // Cromwell uses will fail to decompress. A directory entry is one that ends with a / and
          // has no data.
          final var parentDirectories =
              state.getWorkflowInputFiles().keySet().stream()
                  .flatMap(CromwellWorkflowEngine::findAllParents)
                  .distinct()
                  .sorted(Comparator.comparing(Path::getNameCount))
                  .toList();
          for (final var parentDirectory : parentDirectories) {
            zipFile.putNextEntry(new ZipEntry(parentDirectory.toString() + "/"));
            zipFile.closeEntry();
          }

          for (final var accessory : state.getWorkflowInputFiles().entrySet()) {
            zipFile.putNextEntry(new ZipEntry(accessory.getKey()));
            zipFile.write(accessory.getValue().getBytes(StandardCharsets.UTF_8));
            zipFile.closeEntry();
          }
        }
        final var zipContents = zipOutput.toByteArray();
        body.addPart(
            "workflowDependencies", () -> new ByteArrayInputStream(zipContents), null, null);
      }

      CROMWELL_REQUESTS.labels(url).inc();
      CLIENT
          .sendAsync(
              HttpRequest.newBuilder()
                  .uri(URI.create(String.format("%s/api/workflows/v1", url)))
                  .timeout(Duration.ofMinutes(1))
                  .header("Content-Type", body.getContentType())
                  .POST(body.build())
                  .build(),
              BodyHandlers.ofString(StandardCharsets.UTF_8))
          .thenAccept(
              r -> {
                if (r.statusCode() / 100 != 2) {
                  monitor.permanentFailure(
                      String.format(
                          "Cromwell returned HTTP status %d on submission: %s",
                          r.statusCode(), r.body()));
                  return;
                }
                try {
                  final var result = MAPPER.readValue(r.body(), WorkflowStatusResponse.class);
                  if (result.getId() == null || result.getId().equals("null")) {
                    monitor.permanentFailure("Cromwell failed to launch workflow.");
                    return;
                  }
                  state.setCromwellId(result.getId());
                  monitor.storeRecoveryInformation(state);
                  monitor.updateState(statusFromCromwell(result.getStatus()));
                  monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
                  monitor.log(
                      System.Logger.Level.INFO,
                      String.format(
                          "Started Cromwell workflow %s on %s", state.getCromwellId(), url));
                } catch (JsonProcessingException e) {
                  e.printStackTrace();
                  monitor.permanentFailure(e.getMessage());
                }
              })
          .exceptionally(
              t -> {
                monitor.log(
                    System.Logger.Level.INFO,
                    String.format("Failed to launch Cromwell workflow on %s", url));
                t.printStackTrace();
                CROMWELL_FAILURES.labels(url).inc();
                monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
                return null;
              });
    } catch (Exception e) {
      monitor.log(
          System.Logger.Level.INFO, String.format("Failed to launch Cromwell workflow on %s", url));
      CROMWELL_FAILURES.labels(url).inc();
      monitor.permanentFailure(e.toString());
    }
  }

  @Override
  public void startup() {
    // Always ok
  }

  @Override
  public boolean supports(WorkflowLanguage language) {
    return language == WorkflowLanguage.WDL_1_0 || language == WorkflowLanguage.WDL_1_1;
  }

  public void setDebugInflightRuns(Boolean debugInflightRuns) {
    this.debugInflightRuns = debugInflightRuns;
  }
}
