package ca.on.oicr.gsi.vidarr.cromwell;

import static ca.on.oicr.gsi.vidarr.OperationAction.load;
import static ca.on.oicr.gsi.vidarr.OperationAction.value;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.log;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.onInnerState;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.poll;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.repeatUntilSuccess;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.subStep;
import static ca.on.oicr.gsi.vidarr.OperationStep.debugInfo;
import static ca.on.oicr.gsi.vidarr.OperationStep.http;
import static ca.on.oicr.gsi.vidarr.OperationStep.monitorWhen;
import static ca.on.oicr.gsi.vidarr.OperationStep.requireJsonSuccess;
import static ca.on.oicr.gsi.vidarr.OperationStep.requirePresent;
import static ca.on.oicr.gsi.vidarr.OperationStep.status;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OperationStep;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import ca.on.oicr.gsi.vidarr.WorkingStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.prometheus.client.Counter;
import java.lang.System.Logger.Level;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

/** Run workflows using Cromwell */
public final class CromwellWorkflowEngine implements WorkflowEngine<StateUnstarted, CleanupState> {
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

  public static WorkflowEngineProvider provider() {
    return () -> Stream.of(new Pair<>("cromwell", CromwellWorkflowEngine.class));
  }

  static WorkingStatus statusFromCromwell(String status) {
    return switch (status) {
      case "On Hold" -> WorkingStatus.WAITING;
      case "Submitted" -> WorkingStatus.QUEUED;
      case "Running" -> WorkingStatus.RUNNING;
      default -> WorkingStatus.UNKNOWN;
    };
  }

  // TODO Optimally, this would be Optional<Boolean>
  private boolean debugInflightRuns;
  private Map<String, BasicType> engineParameters;
  private String url;

  public CromwellWorkflowEngine() {}

  @Override
  public OperationAction<?, CleanupState, Void> cleanup() {
    return value(CleanupState.class, null);
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

  public String getUrl() {
    return url;
  }

  @Override
  public OperationAction<?, StateUnstarted, Result<CleanupState>> run() {
    return load(StateUnstarted.class, StateUnstarted::buildLaunchRequest)
        .then(http(new JsonBodyHandler<>(MAPPER, WorkflowStatusResponse.class)))
        .then(
            log(
                Level.INFO,
                (state, response) ->
                    String.format(
                        "Got response %d on %s", response.statusCode(), state.cromwellServer())))
        .then(monitorWhen(CROMWELL_FAILURES, OperationStep::isHttpOk, url))
        .then(requireJsonSuccess())
        .map(result -> Optional.ofNullable(result.getId()).filter(id -> !id.equals("null")))
        .then(requirePresent())
        .then(status(WorkingStatus.QUEUED))
        .then(
            log(
                Level.INFO,
                (state, id) ->
                    String.format(
                        "Started Cromwell workflow %s on %s", id, state.cromwellServer())))
        .then(repeatUntilSuccess(Duration.ofMinutes(10), 5))
        .then(
            subStep(
                onInnerState(StateUnstarted.class, StateUnstarted::checkTask),
                load(StateStarted.class, (state) -> state.buildCheckRequest(debugInflightRuns))
                    .then(http(new JsonBodyHandler<>(MAPPER, WorkflowMetadataResponse.class)))
                    .then(requireJsonSuccess())
                    .then(debugInfo(WorkflowMetadataResponse::debugInfo))
                    .then(
                        log(
                            Level.INFO,
                            (state, response) ->
                                String.format(
                                    "Status of Cromwell workflow %s on %s: %s",
                                    state.cromwellId(),
                                    state.cromwellServer(),
                                    response.getStatus())))
                    .then(status(response -> statusFromCromwell(response.getStatus())))
                    .map(WorkflowMetadataResponse::pollStatus)
                    .then(poll(Duration.ofMinutes(5)))
                    .reload(StateStarted::buildOutputsRequest)
                    .then(http(new JsonBodyHandler<>(MAPPER, WorkflowOutputResponse.class)))
                    .then(requireJsonSuccess())
                    .map(
                        (state, output) ->
                            new Result<>(
                                output.getOutputs(),
                                state.runtimeProvisionerUrl(),
                                Optional.empty()))));
  }

  public void setDebugInflightRuns(boolean debugInflightRuns) {
    this.debugInflightRuns = debugInflightRuns;
  }

  public void setEngineParameters(Map<String, BasicType> engineParameters) {
    this.engineParameters = engineParameters;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public StateUnstarted start(
      WorkflowLanguage workflowLanguage,
      String workflow,
      Stream<Pair<String, String>> accessoryFiles,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters) {
    /* Cromwell and Shesmu/Vidarr handle optional parameters differently.
     * Shesmu/Vidarr encoding missing values as "null", but Cromwell encodes
     * them as absent. If a value is "null", Cromwell will complain that it
     * can't be parsed rather than drop the null and use its default.
     * Therefore, we delete all the nulls so that we have something Cromwell will
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

    return new StateUnstarted(
        url,
        vidarrId,
        engineParameters,
        filteredParameters,
        accessoryFiles.collect(Collectors.toMap(Pair::first, Pair::second)),
        workflowLanguage,
        workflow);
  }

  @Override
  public void startup() {
    // Always ok
  }

  @Override
  public boolean supports(WorkflowLanguage language) {
    return language == WorkflowLanguage.WDL_1_0 || language == WorkflowLanguage.WDL_1_1;
  }
}
