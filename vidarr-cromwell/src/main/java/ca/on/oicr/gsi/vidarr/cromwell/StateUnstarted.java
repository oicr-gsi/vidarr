package ca.on.oicr.gsi.vidarr.cromwell;

import static ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine.MAPPER;

import ca.on.oicr.gsi.vidarr.MultiPartBodyPublisher;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** The current state of a running workflow to be recorded in the database */
public record StateUnstarted(
    String cromwellServer,
    String vidarrId,
    JsonNode engineParameters,
    ObjectNode parameters,
    Map<String, String> workflowInputFiles,
    WorkflowLanguage workflowLanguage,
    String workflowSource) {
  public StateStarted checkTask(String cromwellId) {
    return new StateStarted(cromwellId, cromwellServer);
  }

  public HttpRequest buildLaunchRequest() throws IOException {
    final var body =
        new MultiPartBodyPublisher()
            .addPart("workflowSource", this.workflowSource())
            .addPart("workflowInputs", MAPPER.writeValueAsString(this.parameters()))
            .addPart("workflowType", "WDL")
            .addPart(
                "workflowTypeVersion",
                switch (this.workflowLanguage()) {
                  case WDL_1_0 -> "1.0";
                  case WDL_1_1 -> "1.1";
                  default -> "draft1";
                })
            .addPart(
                "labels",
                MAPPER.writeValueAsString(
                    Collections.singletonMap(
                        "vidarr-id",
                        this.vidarrId().substring(Math.max(0, this.vidarrId().length() - 255)))));
    if (engineParameters != null) {
      // Cromwell will error on null values, so only add if engineParameters are present
      body.addPart("workflowOptions", MAPPER.writeValueAsString(this.engineParameters()));
    }
    if (!this.workflowInputFiles().isEmpty()) {
      // Cromwell doesn't deduplicate these and stores them all in its database,
      // so it doesn't
      // matter if we make the effort to ensure these ZIP files are
      // byte-for-byte identical.
      final var zipOutput = new ByteArrayOutputStream();
      try (final var zipFile = new ZipOutputStream(zipOutput)) {

        // We have to create all the parent directories or the better-files
        // compressor that
        // Cromwell uses will fail to decompress. A directory entry is one that
        // ends with a / and
        // has no data.
        final var parentDirectories =
            this.workflowInputFiles().keySet().stream()
                .flatMap(StateUnstarted::findAllParents)
                .distinct()
                .sorted(Comparator.comparing(Path::getNameCount))
                .toList();
        for (final var parentDirectory : parentDirectories) {
          zipFile.putNextEntry(new ZipEntry(parentDirectory.toString() + "/"));
          zipFile.closeEntry();
        }

        for (final var accessory : this.workflowInputFiles().entrySet()) {
          zipFile.putNextEntry(new ZipEntry(accessory.getKey()));
          zipFile.write(accessory.getValue().getBytes(StandardCharsets.UTF_8));
          zipFile.closeEntry();
        }
      }
      final var zipContents = zipOutput.toByteArray();
      body.addPart("workflowDependencies", () -> new ByteArrayInputStream(zipContents), null, null);
    }
    return HttpRequest.newBuilder()
        .uri(URI.create(String.format("%s/api/workflows/v1", this.cromwellServer())))
        .timeout(Duration.ofMinutes(1))
        .header("Content-Type", body.getContentType())
        .POST(body.build())
        .build();
  }

  private static Stream<Path> findAllParents(String file) {
    final var parents = new ArrayList<Path>();
    for (var path = Path.of(file).getParent(); path != null; path = path.getParent()) {
      parents.add(path);
    }
    return parents.stream();
  }
}
