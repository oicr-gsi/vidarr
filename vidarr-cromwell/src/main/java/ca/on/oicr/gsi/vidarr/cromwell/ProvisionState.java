package ca.on.oicr.gsi.vidarr.cromwell;

import static ca.on.oicr.gsi.vidarr.cromwell.CromwellWorkflowEngine.MAPPER;

import ca.on.oicr.gsi.vidarr.MultiPartBodyPublisher;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/** The current provisioning state, to be recorded in the database */
public record ProvisionState(
    String cromwellUrl, String fileName, JsonNode metadata, String vidarrId) {

  public HttpRequest buildLaunchRequest(CromwellOutputProvisioner provisioner) throws IOException {
    final OutputMetadata outputMetadata = MAPPER.convertValue(metadata, OutputMetadata.class);
    Path path = Path.of(outputMetadata.getOutputDirectory());
    int startIndex = 0;
    for (final int length : provisioner.getChunks()) {
      if (length < 1) {
        break;
      }
      final int endIndex = Math.min(vidarrId.length(), startIndex + length);
      if (endIndex == startIndex) {
        break;
      }
      path = path.resolve(vidarrId.substring(startIndex, endIndex));
      startIndex = endIndex;
    }

    final String outputPrefix = path.resolve(vidarrId).toString();

    final MultiPartBodyPublisher body =
        new MultiPartBodyPublisher()
            .addPart(
                provisioner.getWorkflowUrl() == null ? "workflowSource" : "workflowUrl",
                provisioner.getWorkflowUrl() == null
                    ? provisioner.getWorkflowSource()
                    : provisioner.getWorkflowUrl())
            .addPart("workflowType", "WDL")
            .addPart("workflowTypeVersion", provisioner.getWdlVersion())
            .addPart(
                "labels",
                MAPPER.writeValueAsString(
                    Collections.singletonMap(
                        "vidarr-id", vidarrId.substring(Math.max(0, vidarrId.length() - 255)))))
            .addPart(
                "workflowInputs",
                MAPPER.writeValueAsString(
                    Map.of(
                        provisioner.getFileField(),
                        fileName,
                        provisioner.getOutputPrefixField(),
                        outputPrefix)))
            .addPart(
                "workflowOptions", MAPPER.writeValueAsString(provisioner.getWorkflowOptions()));
    return HttpRequest.newBuilder()
        .uri(URI.create(String.format("%s/api/workflows/v1", cromwellUrl)))
        .timeout(Duration.ofMinutes(1))
        .header("Content-Type", body.getContentType())
        .POST(body.build())
        .build();
  }

  public StateStarted checkTask(String cromwellId) {
    return new StateStarted(cromwellId, cromwellUrl);
  }
}
