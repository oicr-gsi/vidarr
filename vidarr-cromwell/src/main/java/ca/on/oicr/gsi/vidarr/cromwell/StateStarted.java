package ca.on.oicr.gsi.vidarr.cromwell;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;

public record StateStarted(String cromwellId, String cromwellServer) {
  public HttpRequest buildCheckRequest(boolean debugInflightRuns) {
    return HttpRequest.newBuilder()
        .uri(CromwellMetadataURL.formatMetadataURL(cromwellServer, cromwellId, debugInflightRuns))
        .timeout(Duration.ofMinutes(1))
        .GET()
        .build();
  }

  public HttpRequest buildOutputsRequest() {
    return HttpRequest.newBuilder()
        .uri(
            URI.create(String.format("%s/api/workflows/v1/%s/outputs", cromwellServer, cromwellId)))
        .timeout(Duration.ofMinutes(1))
        .GET()
        .build();
  }

  public String runtimeProvisionerUrl() {
    // Note: This instance of the cromwell URL is
    // for use by OutputProvisioners
    // Don't include 'excludeKeys', includeCalls
    // needs to be true
    return CromwellMetadataURL.formatMetadataURL(cromwellServer(), cromwellId(), true).toString();
  }
}
