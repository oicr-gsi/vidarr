package ca.on.oicr.gsi.vidarr.cromwell;

import static ca.on.oicr.gsi.vidarr.OperationAction.load;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.log;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.onInnerState;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.poll;
import static ca.on.oicr.gsi.vidarr.OperationStatefulStep.repeatUntilSuccess;
import static ca.on.oicr.gsi.vidarr.OperationStep.debugInfo;
import static ca.on.oicr.gsi.vidarr.OperationStep.getJson;
import static ca.on.oicr.gsi.vidarr.OperationStep.getResponseBody;
import static ca.on.oicr.gsi.vidarr.OperationStep.handleHttpResponseCode;
import static ca.on.oicr.gsi.vidarr.OperationStep.http;
import static ca.on.oicr.gsi.vidarr.OperationStep.log;
import static ca.on.oicr.gsi.vidarr.OperationStep.mapping;
import static ca.on.oicr.gsi.vidarr.OperationStep.monitorWhen;
import static ca.on.oicr.gsi.vidarr.OperationStep.requireJsonSuccess;
import static ca.on.oicr.gsi.vidarr.OperationStep.requirePresent;
import static ca.on.oicr.gsi.vidarr.OperationStep.sleep;
import static ca.on.oicr.gsi.vidarr.OperationStep.status;
import static ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine.CROMWELL_FAILURES;
import static ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine.MAPPER;
import static ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine.statusFromCromwell;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep.Child;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep.RepeatCounter;
import ca.on.oicr.gsi.vidarr.OperationStep;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkingStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Provision out a file to a user-specified directory for permanent storage using a Cromwell
 * workflow
 */
public class CromwellOutputProvisioner
    implements OutputProvisioner<PreflightState, ProvisionState> {

  static final List<Pair<String, String>> EXTENSION_TO_META_TYPE =
      List.of(
          new Pair<>(".bam", "application/bam"),
          new Pair<>(".bai", "application/bam-index"),
          new Pair<>(".g.vcf.gz", "application/g-vcf-gz"),
          new Pair<>(".json", "text/json"),
          new Pair<>(".pdf", "application/pdf"),
          new Pair<>(".tar.gz", "application/tar-gzip"),
          new Pair<>(".tgz", "application/tar-gzip"),
          new Pair<>(".tbi", "application/tbi"),
          new Pair<>(".vcf.gz", "application/vcf-gz"),
          new Pair<>(".zip", "application/zip-report-bundle"),
          new Pair<>(".fastq.gz", "chemical/seq-na-fastq-gzip"),
          new Pair<>(".fastq", "chemical/seq-na-fastq"),
          new Pair<>(".png", "image/png"),
          new Pair<>(".bed", "text/bed"),
          new Pair<>(".BedGraph", "text/bed"),
          new Pair<>(".fpkm_tracking", "text/fpkm-tracking"),
          new Pair<>(".gtf", "text/gtf"),
          new Pair<>(".html", "text/html"),
          new Pair<>(".vcf", "text/vcf"),
          new Pair<>(".txt.gz", "application/text-gz"),
          new Pair<>(".gz", "application/text-gz"),
          new Pair<>(".out", "text/plain"),
          new Pair<>(".log", "text/plain"),
          new Pair<>(".txt", "text/plain"),
          new Pair<>(".junction", "text/junction"),
          new Pair<>(".seg", "application/seg"),
          new Pair<>(".Rdata", "application/rdata"),
          new Pair<>(".RData", "application/rdata"),
          new Pair<>(".cram", "application/cram"),
          new Pair<>(".crai", "application/cram-index"),
          new Pair<>("", "application/octet-stream"));

  public static OutputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("cromwell", CromwellOutputProvisioner.class));
  }

  private int[] chunks;
  private String cromwellUrl;
  private boolean debugCalls;
  private String fileField;
  private String fileSizeField;
  private String checksumField;
  private String checksumTypeField;
  private String outputPrefixField;
  private String storagePathField;
  private String wdlVersion;
  private ObjectNode workflowOptions = MAPPER.createObjectNode();
  private String workflowSource;
  private String workflowUrl;

  public CromwellOutputProvisioner() {}

  @Override
  public boolean canProvision(OutputProvisionFormat format) {
    return format == OutputProvisionFormat.FILES;
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) {
    sectionRenderer.line("Input Parameter for File Path", fileField);
    sectionRenderer.line("Output Parameter for File Size", fileSizeField);
    sectionRenderer.line("Output Parameter for Final Storage Path", storagePathField);
    sectionRenderer.line("Output Parameter for Checksum", checksumField);
    sectionRenderer.line("Output Parameter for Checksum Type", checksumTypeField);
    sectionRenderer.line("Provision Out WDL Workflow Version", wdlVersion);
    sectionRenderer.link("Cromwell Instance", cromwellUrl, cromwellUrl);
    if (workflowSource == null) {
      sectionRenderer.link("Provision Out Workflow", workflowUrl, workflowUrl);
    } else {
      sectionRenderer.line("Provision Out Workflow", workflowSource);
    }
  }

  private OutputProvisioner.Result extractOutput(
      Child<RepeatCounter<ProvisionState>, ?> state, WorkflowOutputResponse result) {
    return Result.file(
        result.getOutputs().get(storagePathField).asText(),
        result.getOutputs().get(checksumField).asText(),
        result.getOutputs().get(checksumTypeField).asText(),
        Long.parseLong(result.getOutputs().get(fileSizeField).asText()),
        EXTENSION_TO_META_TYPE.stream()
            .filter(p -> state.loadInner(ProvisionState.class).fileName().endsWith(p.first()))
            .findFirst()
            .map(Pair::second)
            .orElseThrow());
  }

  public String getChecksumField() {
    return checksumField;
  }

  public String getChecksumTypeField() {
    return checksumTypeField;
  }

  public int[] getChunks() {
    return chunks;
  }

  public String getCromwellUrl() {
    return cromwellUrl;
  }

  public String getFileField() {
    return fileField;
  }

  public String getFileSizeField() {
    return fileSizeField;
  }

  public String getOutputPrefixField() {
    return outputPrefixField;
  }

  public String getStoragePathField() {
    return storagePathField;
  }

  public String getWdlVersion() {
    return wdlVersion;
  }

  public ObjectNode getWorkflowOptions() {
    return workflowOptions;
  }

  public String getWorkflowSource() {
    return workflowSource;
  }

  public String getWorkflowUrl() {
    return workflowUrl;
  }

  @Override
  public PreflightState preflightCheck(JsonNode metadata) {
    return new PreflightState();
  }

  @Override
  public ProvisionState prepareProvisionInput(
      String workflowRunId, String data, JsonNode metadata) {
    return new ProvisionState(cromwellUrl, data, metadata, workflowRunId);
  }

  @Override
  public OperationAction<?, ProvisionState, OutputProvisioner.Result> build() {
    return load(ProvisionState.class, (state) -> state.buildLaunchRequest(this))
        .then(http(new JsonBodyHandler<>(MAPPER, WorkflowStatusResponse.class)))
        .then(
            log(
                Level.INFO,
                (response) ->
                    String.format("Got response %d on %s", response.statusCode(), cromwellUrl)))
        .then(monitorWhen(CROMWELL_FAILURES, OperationStep::isHttpNotOk, cromwellUrl))
        .then(handleHttpResponseCode())
        .then(repeatUntilSuccess(Duration.ofMinutes(10), 5))
        .then(getJson())
        .map(result -> Optional.ofNullable(result.getId()).filter(id -> !id.equals("null")))
        .then(requirePresent())
        .then(status(WorkingStatus.QUEUED))
        .then(
            log(
                Level.INFO,
                id -> String.format("Started Cromwell provision-out %s on %s", id, cromwellUrl)))
        .then(sleep(Duration.ofSeconds(30)))
        .then(
            OperationStatefulStep.subStep(
                onInnerState(ProvisionState.class, ProvisionState::checkTask),
                load(StateStarted.class, (state) -> state.buildCheckRequest(debugCalls))
                    .then(http(new JsonBodyHandler<>(MAPPER, WorkflowMetadataResponse.class)))
                    .then(monitorWhen(CROMWELL_FAILURES, OperationStep::isHttpNotOk, cromwellUrl))
                    .then(handleHttpResponseCode())
                    .then(repeatUntilSuccess(Duration.ofMinutes(5), 5))
                    .then(getJson())
                    .then(debugInfo(WorkflowMetadataResponse::debugInfo))
                    .then(
                        log(
                            Level.INFO,
                            (state, response) ->
                                String.format(
                                    "Status of Cromwell provision-out %s on %s: %s",
                                    state.loadInner(StateStarted.class).cromwellId(),
                                    state.loadInner(StateStarted.class).cromwellServer(),
                                    response.getStatus())))
                    .then(status(response -> statusFromCromwell(response.getStatus())))
                    .map(WorkflowMetadataResponse::pollStatus)
                    .then(poll(Duration.ofMinutes(5)))
                    .reload(s -> s.loadInner(StateStarted.class).buildOutputsRequest())
                    .then(http(new JsonBodyHandler<>(MAPPER, WorkflowOutputResponse.class)))
                    .then(monitorWhen(CROMWELL_FAILURES, OperationStep::isHttpNotOk, cromwellUrl))
                    .then(handleHttpResponseCode())
                    .then(repeatUntilSuccess(Duration.ofMinutes(5), 5))
                    .then(getJson())))
        .map(this::extractOutput);
  }

  @Override
  public OperationAction<?, PreflightState, Boolean> buildPreflight() {
    return OperationAction.value(PreflightState.class, true);
  }

  public void setChecksumField(String checksumField) {
    this.checksumField = checksumField;
  }

  public void setChecksumTypeField(String checksumTypeField) {
    this.checksumTypeField = checksumTypeField;
  }

  public void setChunks(int[] chunks) {
    this.chunks = chunks;
  }

  public void setCromwellUrl(String cromwellUrl) {
    this.cromwellUrl = cromwellUrl;
  }

  public void setDebugCalls(boolean debugCalls) {
    this.debugCalls = debugCalls;
  }

  public void setFileField(String fileField) {
    this.fileField = fileField;
  }

  public void setFileSizeField(String fileSizeField) {
    this.fileSizeField = fileSizeField;
  }

  public void setOutputPrefixField(String outputPrefixField) {
    this.outputPrefixField = outputPrefixField;
  }

  public void setStoragePathField(String storagePathField) {
    this.storagePathField = storagePathField;
  }

  public void setWdlVersion(String wdlVersion) {
    this.wdlVersion = wdlVersion;
  }

  public void setWorkflowOptions(ObjectNode workflowOptions) {
    this.workflowOptions = workflowOptions;
  }

  public void setWorkflowSource(String workflowSource) {
    this.workflowSource = workflowSource;
  }

  public void setWorkflowUrl(String workflowUrl) {
    this.workflowUrl = workflowUrl;
  }

  @Override
  public void startup() {
    if ((workflowUrl == null) == (workflowSource == null)) {
      throw new IllegalArgumentException(
          "One of workflowUrl or workflowSource must be supplied to Cromwell provision out"
              + " plugin.");
    }
  }

  @Override
  public String type() {
    return "cromwell";
  }

  @Override
  public BasicType typeFor(OutputProvisionFormat format) {
    if (format == OutputProvisionFormat.FILES) {
      return BasicType.object(new Pair<>("outputDirectory", BasicType.STRING));
    } else {
      throw new IllegalArgumentException("Cannot provision non-file output");
    }
  }
}
