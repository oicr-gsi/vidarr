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
import java.util.stream.Stream;

/**
 * Provision out a file to a user-specified directory for permanent storage using a Cromwell
 * workflow
 */
public class CromwellOutputProvisioner
    extends BaseJsonOutputProvisioner<OutputMetadata, ProvisionState, Void> {
  private static final int CHECK_DELAY = 1;
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
  private String fileField;
  private String fileSizeField;
  private String md5Field;
  private String outputPrefixField;
  private String storagePathField;
  private String wdlVersion;
  private ObjectNode workflowOptions = MAPPER.createObjectNode();
  private String workflowSource;
  private String workflowUrl;
  private Boolean debugCalls;

  public CromwellOutputProvisioner() {
    super(MAPPER, ProvisionState.class, Void.class, OutputMetadata.class);
  }

  @Override
  public boolean canProvision(OutputProvisionFormat format) {
    return format == OutputProvisionFormat.FILES;
  }

  private void check(
      ProvisionState state, WorkMonitor<OutputProvisioner.Result, ProvisionState> monitor) {
    try {
      monitor.log(
          System.Logger.Level.INFO,
          String.format("Checking Cromwell job %s on %s", state.getCromwellId(), cromwellUrl));
      CROMWELL_REQUESTS.labels(cromwellUrl).inc();
      CLIENT
          .sendAsync(
              HttpRequest.newBuilder()
                  .uri(CromwellMetadataURL.formatMetadataURL(cromwellUrl, state.getCromwellId(), debugCalls))
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
                        "Cromwell job %s on %s is in state %s",
                        state.getCromwellId(), cromwellUrl, result.getStatus()));
                monitor.storeDebugInfo(result.debugInfo());
                switch (result.getStatus()) {
                    // In the case of failures ("Aborted" or "Failed"), request the full metadata
                    // from Cromwell if we don't already have it
                    // so we can have call info for debugging.
                  case "Aborted":
                  case "Failed":
                    if (debugCalls){
                      monitor.log(
                              System.Logger.Level.INFO,
                              String.format("Cromwell job %s is failed. Cromwell OutputProvisioner "
                                  + "is configured to have already fetched calls info. Skipping "
                                  + "second request.", state.getCromwellId())
                      );
                      monitor.permanentFailure("Cromwell failure: " + result.getStatus());
                      break;
                    }
                    monitor.log(
                        System.Logger.Level.INFO,
                        String.format(
                            "Cromwell job %s is failed, fetching call info on %s",
                            state.getCromwellId(), cromwellUrl));
                    CROMWELL_REQUESTS.labels(cromwellUrl).inc();

                    CLIENT
                        .sendAsync(
                            HttpRequest.newBuilder()
                                .uri(CromwellMetadataURL.formatMetadataURL(cromwellUrl, state.getCromwellId(), true))
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
                                      state.getCromwellId(), cromwellUrl));
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
                                      state.getCromwellId(), cromwellUrl, t2.getMessage()));
                              CROMWELL_FAILURES.labels(cromwellUrl).inc();

                              // TODO: this may schedule 2 requests to cromwell /metadata now. Consider
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
                    monitor.scheduleTask(
                        CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
                }
              })
          .exceptionally(
              t -> {
                t.printStackTrace();
                monitor.log(
                    System.Logger.Level.WARNING,
                    String.format(
                        "Failed to get Cromwell job %s on %s due to %s",
                        state.getCromwellId(), cromwellUrl, t.getMessage()));
                CROMWELL_FAILURES.labels(cromwellUrl).inc();
                monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
                return null;
              });
    } catch (Exception e) {
      e.printStackTrace();
      monitor.log(
          System.Logger.Level.WARNING,
          String.format(
              "Failed to get Cromwell job %s on %s due to %s",
              state.getCromwellId(), cromwellUrl, e.getMessage()));
      CROMWELL_FAILURES.labels(cromwellUrl).inc();
      monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
    }
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) {
    sectionRenderer.line("Input Parameter for File Path", fileField);
    sectionRenderer.line("Output Parameter for File Size", fileSizeField);
    sectionRenderer.line("Output Parameter for Final Storage Path", storagePathField);
    sectionRenderer.line("Output Parameter for MD5", md5Field);
    sectionRenderer.line("Provision Out WDL Workflow Version", wdlVersion);
    sectionRenderer.link("Cromwell Instance", cromwellUrl, cromwellUrl);
    if (workflowSource == null) {
      sectionRenderer.link("Provision Out Workflow", workflowUrl, workflowUrl);
    } else {
      sectionRenderer.line("Provision Out Workflow", workflowSource);
    }
  }

  private void finish(
      ProvisionState state, WorkMonitor<OutputProvisioner.Result, ProvisionState> monitor) {
    monitor.log(
        System.Logger.Level.INFO,
        String.format(
            "Reaping results of Cromwell job %s on %s", state.getCromwellId(), cromwellUrl));
    CROMWELL_REQUESTS.labels(cromwellUrl).inc();
    CLIENT
        .sendAsync(
            HttpRequest.newBuilder()
                .uri(
                    URI.create(
                        String.format(
                            "%s/api/workflows/v1/%s/outputs", cromwellUrl, state.getCromwellId())))
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
                      "Got results of Cromwell job %s on %s", state.getCromwellId(), cromwellUrl));
              monitor.complete(
                  Result.file(
                      result.getOutputs().get(storagePathField).asText(),
                      result.getOutputs().get(md5Field).asText(),
                      Long.parseLong(result.getOutputs().get(fileSizeField).asText()),
                      state.getMetaType()));
            })
        .exceptionally(
            t -> {
              monitor.log(
                  System.Logger.Level.WARNING,
                  String.format(
                      "Failed to get results of Cromwell job %s on %s",
                      state.getCromwellId(), cromwellUrl));
              t.printStackTrace();
              CROMWELL_FAILURES.labels(cromwellUrl).inc();
              monitor.scheduleTask(CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
              return null;
            });
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

  public String getMd5Field() {
    return md5Field;
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
    state.setVidarrId(workflowId);
    var path = Path.of(metadata.getOutputDirectory());
    int startIndex = 0;
    for (final var length : chunks) {
      if (length < 1) {
        break;
      }
      final var endIndex = Math.min(workflowId.length(), startIndex + length);
      if (endIndex == startIndex) {
        break;
      }
      path = path.resolve(workflowId.substring(startIndex, endIndex));
      startIndex = endIndex;
    }

    state.setOutputPrefix(path.resolve(workflowId).toString());

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
      monitor.scheduleTask(
          () -> {
            try {
              monitor.log(
                  System.Logger.Level.INFO,
                  String.format(
                      "Launching provisioning out job on Cromwell %s for %s",
                      cromwellUrl, state.getFileName()));
              final var body =
                  new MultiPartBodyPublisher()
                      .addPart(
                          workflowUrl == null ? "workflowSource" : "workflowUrl",
                          workflowUrl == null ? workflowSource : workflowUrl)
                      .addPart(
                          "labels",
                          MAPPER.writeValueAsString(
                              Collections.singletonMap(
                                  "vidarr-id",
                                  state
                                      .getVidarrId()
                                      .substring(Math.max(0, state.getVidarrId().length() - 255)))))
                      .addPart(
                          "workflowInputs",
                          MAPPER.writeValueAsString(
                              Map.of(
                                  fileField,
                                  state.getFileName(),
                                  outputPrefixField,
                                  state.getOutputPrefix())))
                      .addPart("workflowOptions", MAPPER.writeValueAsString(workflowOptions))
                      .addPart("workflowType", "WDL")
                      .addPart("workflowTypeVersion", wdlVersion);
              CROMWELL_REQUESTS.labels(cromwellUrl).inc();
              CLIENT
                  .sendAsync(
                      HttpRequest.newBuilder()
                          .uri(URI.create(String.format("%s/api/workflows/v1", cromwellUrl)))
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
                        monitor.scheduleTask(
                            CHECK_DELAY, TimeUnit.MINUTES, () -> check(state, monitor));
                        monitor.log(
                            System.Logger.Level.INFO,
                            String.format(
                                "Provisioning for %s on Cromwell %s is %s",
                                state.getFileName(), cromwellUrl, result.getId()));
                      })
                  .exceptionally(
                      t -> {
                        monitor.log(
                            System.Logger.Level.WARNING,
                            String.format(
                                "Failed to launch provisioning out job on Cromwell %s for %s",
                                cromwellUrl, state.getFileName()));
                        t.printStackTrace();
                        CROMWELL_FAILURES.labels(cromwellUrl).inc();

                        // Call recover() rather than check(): recover() starts with a check for
                        // a null cromwell ID. There's a chance Exception t is 'header parser
                        // received no bytes', or another case where we don't have a cromwell id.
                        // Prevents looping 'Checking Cromwell job null'. recover() calls check()
                        // if a cromwell id is present.
                        monitor.scheduleTask(
                            CHECK_DELAY, TimeUnit.MINUTES, () -> recover(state, monitor));
                        return null;
                      });
            } catch (Exception e) {
              CROMWELL_FAILURES.labels(cromwellUrl).inc();
              monitor.permanentFailure(e.toString());
            }
          });
    } else {
      check(state, monitor);
    }
  }

  public void setChunks(int[] chunks) {
    this.chunks = chunks;
  }

  public void setCromwellUrl(String cromwellUrl) {
    this.cromwellUrl = cromwellUrl;
  }

  public void setFileField(String fileField) {
    this.fileField = fileField;
  }

  public void setFileSizeField(String fileSizeField) {
    this.fileSizeField = fileSizeField;
  }

  public void setMd5Field(String md5Field) {
    this.md5Field = md5Field;
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

  public void setDebugCalls(Boolean debugCalls) {
    this.debugCalls = debugCalls;
  }
}
