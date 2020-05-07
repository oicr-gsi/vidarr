package ca.on.oicr.gsi.vidarr.cromwell;

import static ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine.*;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Provision out a file to a user-specified directory for permanent storage using a Cromwell
 * workflow
 */
public class CromwellOutputProvisioner
    extends BaseJsonOutputProvisioner<OutputMetadata, ProvisionState, Void> {
  public static OutputProvisionerProvider provider() {
    return new OutputProvisionerProvider() {
      @Override
      public OutputProvisioner readConfiguration(ObjectNode node) {
        return new CromwellOutputProvisioner(
            node.get("cromwellUrl").asText(),
            node.get("fileField").asText(),
            node.get("fileSizeField").asText(),
            node.get("md5Field").asText(),
            node.get("outputPrefixField").asText(),
            node.get("storagePathField").asText(),
            node.get("wdlVersion").asText(),
            node.has("workflowOptions")
                ? (ObjectNode) node.get("workflowOptions")
                : MAPPER.createObjectNode(),
            node.get("workflowUrl").asText());
      }

      @Override
      public String type() {
        return "cromwell";
      }
    };
  }

  private static final List<Pair<String, String>> EXTENSION_TO_META_TYPE =
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
          new Pair<>(".txt.gz", "application/txt-gz"),
          new Pair<>(".gz", "application/txt-gz"),
          new Pair<>(".out", "txt/plain"),
          new Pair<>(".log", "txt/plain"),
          new Pair<>(".txt", "txt/plain"),
          new Pair<>(".junction", "txt/junction"),
          new Pair<>(".seg", "application/seg"),
          new Pair<>(".Rdata", "application/rdata"),
          new Pair<>(".RData", "application/rdata"),
          new Pair<>("", "application/octet-stream"));
  private final String baseUrl;
  private final String fileField;
  private final String fileSizeField;
  private final String md5Field;
  private final String outputDirectoryField;
  private final String storagePathField;
  private final String wdlVersion;
  private final ObjectNode workflowOptions;
  private final String workflowUrl;

  public CromwellOutputProvisioner(
      String baseUrl,
      String fileField,
      String fileSizeField,
      String md5Field,
      String outputDirectoryField,
      String storagePathField,
      String wdlVersion,
      ObjectNode workflowOptions,
      String workflowUrl) {
    super(MAPPER, ProvisionState.class, Void.class, OutputMetadata.class);
    this.baseUrl = baseUrl;
    this.fileField = fileField;
    this.fileSizeField = fileSizeField;
    this.md5Field = md5Field;
    this.outputDirectoryField = outputDirectoryField;
    this.storagePathField = storagePathField;
    this.wdlVersion = wdlVersion;
    this.workflowOptions = workflowOptions;
    this.workflowUrl = workflowUrl;
  }

  @Override
  public boolean canProvision(OutputProvisionFormat format) {
    return format == OutputProvisionFormat.FILES;
  }

  private void check(
      ProvisionState state, WorkMonitor<OutputProvisioner.Result, ProvisionState> monitor) {
    try {
      CROMWELL_REQUESTS.labels(baseUrl).inc();
      CLIENT
          .sendAsync(
              HttpRequest.newBuilder()
                  .uri(
                      URI.create(
                          String.format(
                              "%s/api/workflows/v1/%s/status", baseUrl, state.getCromwellId())))
                  .timeout(Duration.ofMinutes(1))
                  .GET()
                  .build(),
              new JsonBodyHandler<>(MAPPER, WorkflowStatusResponse.class))
          .thenApply(HttpResponse::body)
          .thenAccept(
              s -> {
                final var result = s.get();
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
                    monitor.scheduleTask(10, TimeUnit.MINUTES, () -> check(state, monitor));
                }
              })
          .exceptionally(
              t -> {
                t.printStackTrace();
                CROMWELL_FAILURES.labels(baseUrl).inc();
                monitor.scheduleTask(10, TimeUnit.MINUTES, () -> check(state, monitor));
                return null;
              });
    } catch (Exception e) {
      e.printStackTrace();
      CROMWELL_FAILURES.labels(baseUrl).inc();
      monitor.scheduleTask(10, TimeUnit.MINUTES, () -> check(state, monitor));
    }
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) {
    sectionRenderer.line("Input Parameter for File Path", fileField);
    sectionRenderer.line("Output Parameter for File Size", fileSizeField);
    sectionRenderer.line("Output Parameter for Final Storage Path", storagePathField);
    sectionRenderer.line("Output Parameter for MD5", md5Field);
    sectionRenderer.line("Provision Out WDL Workflow Version", wdlVersion);
    sectionRenderer.link("Cromwell Instance", baseUrl, baseUrl);
    sectionRenderer.link("Provision Out Workflow", workflowUrl, workflowUrl);
  }

  private void finish(
      ProvisionState state, WorkMonitor<OutputProvisioner.Result, ProvisionState> monitor) {
    CROMWELL_REQUESTS.labels(baseUrl).inc();
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
              monitor.complete(
                  Result.file(
                      result.getOutputs().get(storagePathField).asText(),
                      result.getOutputs().get(md5Field).asText(),
                      Long.parseLong(result.getOutputs().get(fileSizeField).asText()),
                      state.getMetaType()));
            })
        .exceptionally(
            t -> {
              t.printStackTrace();
              CROMWELL_FAILURES.labels(baseUrl).inc();
              monitor.scheduleTask(10, TimeUnit.MINUTES, () -> check(state, monitor));
              return null;
            });
  }

  @Override
  protected Void preflightCheck(
      OutputMetadata metadata, WorkMonitor<Boolean, ProvisionState> monitor) {
    monitor.scheduleTask(() -> monitor.complete(true));
    return null;
  }

  @Override
  protected void preflightRecover(Void state, WorkMonitor<Boolean, ProvisionState> monitor) {
    monitor.scheduleTask(() -> monitor.complete(true));
  }

  @Override
  protected ProvisionState provision(
      String workflowId,
      String data,
      OutputMetadata metadata,
      WorkMonitor<Result, ProvisionState> monitor) {
    final var state = new ProvisionState();
    state.setFileName(data);
    state.setOutputPrefix(Path.of(metadata.getOutputDirectory()).resolve(workflowId).toString());

    state.setMetaType(
        EXTENSION_TO_META_TYPE.stream()
            .filter(p -> data.endsWith(p.first()))
            .findFirst()
            .map(Pair::second)
            .orElseThrow());
    recover(state, monitor);
    return state;
  }

  @Override
  protected void recover(ProvisionState state, WorkMonitor<Result, ProvisionState> monitor) {
    if (state.getCromwellId() == null) {
      try {
        final var body =
            new MultiPartBodyPublisher()
                .addPart("workflowUrl", workflowUrl)
                .addPart(
                    "workflowInputs",
                    MAPPER.writeValueAsString(
                        Map.of(
                            fileField,
                            state.getFileName(),
                            outputDirectoryField,
                            state.getOutputPrefix())))
                .addPart("workflowOptions", MAPPER.writeValueAsString(workflowOptions))
                .addPart("workflowType", "WDL")
                .addPart("workflowTypeVersion", wdlVersion);
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
                  state.setCromwellId(result.getId());
                  monitor.updateState(statusFromCromwell(result.getStatus()));
                  monitor.scheduleTask(10, TimeUnit.MINUTES, () -> check(state, monitor));
                })
            .exceptionally(
                t -> {
                  t.printStackTrace();
                  CROMWELL_FAILURES.labels(baseUrl).inc();
                  monitor.scheduleTask(10, TimeUnit.MINUTES, () -> check(state, monitor));
                  return null;
                });
      } catch (Exception e) {
        CROMWELL_FAILURES.labels(baseUrl).inc();
        monitor.permanentFailure(e.toString());
      }
    } else {
      check(state, monitor);
    }
  }

  @Override
  public String type() {
    return "cromwell";
  }

  @Override
  public SimpleType typeFor(OutputProvisionFormat format) {
    if (format == OutputProvisionFormat.FILES) {
      return InputType.object(new Pair<>("outputDirectory", InputType.STRING));
    } else {
      throw new IllegalArgumentException("Cannot provision non-file output");
    }
  }
}
