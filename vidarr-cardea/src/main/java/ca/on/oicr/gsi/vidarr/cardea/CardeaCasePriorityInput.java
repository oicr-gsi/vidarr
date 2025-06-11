package ca.on.oicr.gsi.vidarr.cardea;

import ca.on.oicr.gsi.cache.KeyValueCache;
import ca.on.oicr.gsi.cache.SimpleRecord;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.prometheus.client.Counter;
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

public class CardeaCasePriorityInput implements PriorityInput {

  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new Jdk8Module())
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  private int defaultPriority;
  private BasicType schema;
  private int ttl = 60;
  private String baseUrl;
  private KeyValueCache<String, Optional<Integer>> values;
  private static final String casePriorityUrlFormat = "%s/cases/%s/priority";

  static final Counter CARDEA_CASE_ID_PRIORITY_FAILED =
      Counter.build(
              "vidarr_cardea_priority_by_case_id_failed_requests",
              "The number of failed Cardea priority by case id requests")
          .labelNames("target")
          .register();

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    try {
      return values.get(MAPPER.writeValueAsString(input)).orElse(defaultPriority);
    } catch (JacksonException e) {
      CARDEA_CASE_ID_PRIORITY_FAILED.labels(baseUrl).inc();
      e.printStackTrace();
      return defaultPriority;
    }
  }

  public int getDefaultPriority() {
    return defaultPriority;
  }

  public BasicType getSchema() {
    return schema;
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

  public void setSchema(BasicType schema) {
    this.schema = schema;
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
            return HTTP_CLIENT
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
                    new JsonBodyHandler<>(MAPPER, new TypeReference<Optional<Integer>>() {}))
                .body()
                .get();
          }
        };
  }
}
