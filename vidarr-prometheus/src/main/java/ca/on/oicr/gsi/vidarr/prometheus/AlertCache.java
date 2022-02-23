package ca.on.oicr.gsi.vidarr.prometheus;

import ca.on.oicr.gsi.cache.ReplacingRecord;
import ca.on.oicr.gsi.cache.ValueCache;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

public class AlertCache extends ValueCache<Stream<AlertDto>> {
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private final String alertmanagerUrl;
  private final ObjectMapper MAPPER;
  private final Integer requestTimeout;

  public AlertCache(
      String name,
      String alertManagerUrl,
      Integer requestTimeout,
      Integer ttl,
      ObjectMapper mapper) {
    super("alertmanager " + name, ttl, ReplacingRecord::new);
    this.alertmanagerUrl = alertManagerUrl;
    this.requestTimeout = requestTimeout;
    this.MAPPER = mapper;
  }

  protected Stream<AlertDto> fetch(Instant lastUpdated) throws Exception {
    if (alertmanagerUrl == null) {
      return Stream.empty();
    }
    var response =
        HTTP_CLIENT.send(
            HttpRequest.newBuilder(URI.create(String.format("%s/api/v1/alerts", alertmanagerUrl)))
                .GET()
                .timeout(Duration.ofMinutes(requestTimeout))
                .build(),
            new JsonBodyHandler<>(MAPPER, AlertResultDto.class));
    final var result = response.body().get();
    if (result == null || result.getData() == null) {
      return Stream.empty();
    }
    return result.getData().stream();
  }
}
