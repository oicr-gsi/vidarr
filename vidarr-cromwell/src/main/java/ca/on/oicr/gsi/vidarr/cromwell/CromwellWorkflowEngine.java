package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prometheus.client.Counter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.xml.stream.XMLStreamException;

/** Run workflows using Cromwell */
public final class CromwellWorkflowEngine
    extends BaseJsonWorkflowEngine<EngineState, String, Void> {
  public static WorkflowEngineProvider provider() {
    return new WorkflowEngineProvider() {
      @Override
      public WorkflowEngine readConfiguration(ObjectNode node) {
        var engineParameters = Optional.<BasicType>empty();
        if (node.has("engineParameters")) {
          engineParameters =
              Optional.ofNullable(
                  MAPPER.convertValue(node.get("engineParameters"), BasicType.class));
        }
        return new CromwellWorkflowEngine(node.get("url").asText(), engineParameters);
      }

      @Override
      public String type() {
        return "cromwell";
      }
    };
  }

  static WorkMonitor.Status statusFromCromwell(String status) {
    return switch (status) {
      case "On Hold" -> WorkMonitor.Status.WAITING;
      case "Submitted" -> WorkMonitor.Status.QUEUED;
      case "Running" -> WorkMonitor.Status.RUNNING;
      default -> WorkMonitor.Status.UNKNOWN;
    };
  }

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
  private final String baseUrl;
  private final Optional<BasicType> engineParameters;

  protected CromwellWorkflowEngine(String baseUrl, Optional<BasicType> engineParameters) {
    super(MAPPER, EngineState.class, String.class, Void.class);
    this.baseUrl = baseUrl;
    this.engineParameters = engineParameters;
  }

  private void check(EngineState state, WorkMonitor<Result<String>, EngineState> monitor) {
    try {
      monitor.log(
          System.Logger.Level.INFO,
          String.format("Checking Cromwell workflow %s on %s", state.getCromwellId(), baseUrl));
      CROMWELL_REQUESTS.labels(baseUrl).inc();
      CLIENT
          .sendAsync(
              HttpRequest.newBuilder()
                  .uri(
                      URI.create(
                          String.format(
                              "%s/api/workflows/v1/%s/metadata", baseUrl, state.getCromwellId())))
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
                        state.getCromwellId(), baseUrl, result.getStatus()));
                monitor.storeDebugInfo(result.debugInfo());
                switch (result.getStatus()) {
                  case "Aborted":
                  case "Failed":
                    monitor.permanentFailure("Cromwell failure: " + result.getStatus());
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
                        "Failed to get status for Cromwell workflow %s on %s",
                        state.getCromwellId(), baseUrl));
                CROMWELL_FAILURES.labels(baseUrl).inc();
                monitor.scheduleTask(5, TimeUnit.MINUTES, () -> check(state, monitor));
                return null;
              });
    } catch (Exception e) {
      e.printStackTrace();
      monitor.log(
          System.Logger.Level.WARNING,
          String.format(
              "Failed to get status for Cromwell workflow %s on %s",
              state.getCromwellId(), baseUrl));
      CROMWELL_FAILURES.labels(baseUrl).inc();
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
    sectionRenderer.link("Server", baseUrl, baseUrl);
  }

  @Override
  public Optional<BasicType> engineParameters() {
    return engineParameters;
  }

  @Override
  public boolean supports(WorkflowLanguage language) {
    return language == WorkflowLanguage.WDL_1_0 || language == WorkflowLanguage.WDL_1_1;
  }

  private void finish(EngineState state, WorkMonitor<Result<String>, EngineState> monitor) {
    CROMWELL_REQUESTS.labels(baseUrl).inc();
    monitor.log(
        System.Logger.Level.INFO,
        String.format(
            "Reaping output of Cromwell workflow %s on %s", state.getCromwellId(), baseUrl));
    CLIENT
        .sendAsync(
            HttpRequest.newBuilder()
                .uri(
                    URI.create(
                        String.format(
                            "%s/api/workflows/v1/%s/outputs", baseUrl, state.getCromwellId())))
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
                      "Got output of Cromwell workflow %s on %s", state.getCromwellId(), baseUrl));
              monitor.complete(
                  new Result<>(
                      result.getOutputs(),
                      String.format(
                          "%s/api/workflows/v1/%s/metadata", baseUrl, state.getCromwellId()),
                      Optional.empty()));
            })
        .exceptionally(
            t -> {
              t.printStackTrace();
              monitor.log(
                  System.Logger.Level.INFO,
                  String.format(
                      "Failed to get output of Cromwell workflow %s on %s",
                      state.getCromwellId(), baseUrl));
              CROMWELL_FAILURES.labels(baseUrl).inc();
              monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
              return null;
            });
  }

  @Override
  protected void recover(EngineState state, WorkMonitor<Result<String>, EngineState> monitor) {
    if (state.getCromwellId() == null) {
      startTask(state, monitor);
    } else {
      check(state, monitor);
    }
  }

  private void startTask(EngineState state, WorkMonitor<Result<String>, EngineState> monitor) {
    try {
      monitor.log(
          System.Logger.Level.INFO, String.format("Starting Cromwell workflow on %s", baseUrl));
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
      CROMWELL_REQUESTS.labels(baseUrl).inc();
      CLIENT
          .sendAsync(
              HttpRequest.newBuilder()
                  .uri(URI.create(String.format("%s/api/workflows/v1", baseUrl)))
                  .timeout(Duration.ofMinutes(1))
                  .header("Content-Type", body.getContentType())
                  .POST(body.build())
                  .build(),
              new JsonBodyHandler<>(MAPPER, WorkflowStatusResponse.class))
          .thenApply(HttpResponse::body)
          .thenAccept(
              s -> {
                final var result = s.get();
                if (result.getId() == null) {
                  monitor.permanentFailure("Cromwell to launch workflow.");
                  return;
                }
                state.setCromwellId(result.getId());
                monitor.storeRecoveryInformation(state);
                monitor.updateState(statusFromCromwell(result.getStatus()));
                monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
                monitor.log(
                    System.Logger.Level.INFO,
                    String.format(
                        "Started Cromwell workflow %s on %s", state.getCromwellId(), baseUrl));
              })
          .exceptionally(
              t -> {
                monitor.log(
                    System.Logger.Level.INFO,
                    String.format("Failed to launch Cromwell workflow on %s", baseUrl));
                t.printStackTrace();
                CROMWELL_FAILURES.labels(baseUrl).inc();
                monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
                return null;
              });
    } catch (Exception e) {
      monitor.log(
          System.Logger.Level.INFO,
          String.format("Failed to launch Cromwell workflow on %s", baseUrl));
      CROMWELL_FAILURES.labels(baseUrl).inc();
      monitor.permanentFailure(e.toString());
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
    state.setEngineParameters(engineParameters);
    state.setParameters(filteredParameters);
    state.setVidarrId(vidarrId);
    state.setWorkflowLanguage(workflowLanguage);
    state.setWorkflowSource(workflow);
    monitor.scheduleTask(() -> startTask(state, monitor));
    return state;
  }
}
