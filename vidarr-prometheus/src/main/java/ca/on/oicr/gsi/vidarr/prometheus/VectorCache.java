package ca.on.oicr.gsi.vidarr.prometheus;

import static ca.on.oicr.gsi.vidarr.prometheus.AlertmanagerAutoInhibitConsumableResource.MAPPER;

import ca.on.oicr.gsi.cache.ReplacingRecord;
import ca.on.oicr.gsi.cache.ValueCache;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

public final class VectorCache extends ValueCache<Stream<VectorResultDto>> {
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private final String prometheusUrl;
  private final String query;
  private final Integer requestTimeout;

  public VectorCache(
      String name, String prometheusUrl, String query, Integer requestTimeout, Integer ttl) {
    super("prometheus " + name, ttl, ReplacingRecord::new);
    this.prometheusUrl = prometheusUrl;
    this.query = query;
    this.requestTimeout = requestTimeout;
  }

  protected Stream<VectorResultDto> fetch(Instant lastUpdated) throws Exception {
    if (prometheusUrl == null) {
      return Stream.empty();
    }
    var response =
        HTTP_CLIENT.send(
            HttpRequest.newBuilder(URI.create(String.format("%s/api/v1/query", prometheusUrl)))
                .POST(
                    BodyPublishers.ofString(
                        "query=" + URLEncoder.encode(query, StandardCharsets.UTF_8)))
                .timeout(Duration.ofMinutes(requestTimeout))
                .build(),
            new JsonBodyHandler<>(MAPPER, QueryResponseDto.class));
    final var result = response.body().get();
    if (result == null || result.getData() == null) {
      return Stream.empty();
    }
    return result.getData().getResult().stream();
  }
}
