package ca.on.oicr.gsi.vidarr.cardea;

import ca.on.oicr.gsi.cache.KeyValueCache;
import ca.on.oicr.gsi.cache.SimpleRecord;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.prometheus.client.Counter;
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;
import java.lang.System.Logger.Level;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

public class CardeaCasePriorityInput implements PriorityInput {

  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
  private int defaultPriority;
  private int ttl = 60;
  private String baseUrl;
  private KeyValueCache<String, Optional<Integer>> values;
  private static final String casePriorityUrlFormat = "%s/cases/%s/priority";

  static final Counter CARDEA_CASE_ID_UNKNOWN =
      Counter.build(
              "vidarr_cardea_case_id_unknown",
              "The number requests where the case id is unknown to Cardea")
          .labelNames("target")
          .register();

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    return values.get(input.asText()).orElse(defaultPriority);
  }

  public int getDefaultPriority() {
    return defaultPriority;
  }

  public int getTtl() {
    return ttl;
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  @Override
  public Optional<HttpHandler> httpHandler() {
    return Optional.of(Handlers.routing().delete("/", this::reset));
  }

  @Override
  public BasicType inputFromSubmitter() {
    return BasicType.STRING;
  }

  private void reset(HttpServerExchange exchange) {
    exchange.setStatusCode(StatusCodes.NO_CONTENT);
    values.invalidateAll();
  }

  public void setDefaultPriority(int defaultPriority) {
    this.defaultPriority = defaultPriority;
  }

  public void setTtl(int ttl) {
    this.ttl = ttl;
  }

  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  @Override
  public void startup(String resourceName, String inputName) {
    values =
        new KeyValueCache<>(resourceName + " " + inputName, ttl, SimpleRecord::new) {
          @Override
          protected Optional<Integer> fetch(String caseId, Instant lastUpdated) throws Exception {
            HttpResponse<Supplier<Optional<Integer>>> response = HTTP_CLIENT
                .send(
                    HttpRequest.newBuilder(
                            URI.create(
                                String.format(
                                    casePriorityUrlFormat,
                                    baseUrl,
                                    URLEncoder.encode(caseId, StandardCharsets.UTF_8)
                                        .replace("+", "%20"))))
                        .header("Content-type", "application/json")
                        .GET()
                        .build(),
                    new JsonBodyHandler<>(MAPPER, new TypeReference<>() {
                    }));
            if (response.statusCode() == 404) {
              System.err.printf("%s: caseId=\"%s\" not found at %s\n", Level.WARNING, caseId, baseUrl);
              CARDEA_CASE_ID_UNKNOWN.labels(baseUrl).inc();
              // compute() will determine what priority should be returned
              return Optional.empty();
            }
            return response.body().get();
          }
        };
  }
}