package ca.on.oicr.gsi.vidarr.core;

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
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.StatusCodes;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

public final class RemotePriorityInput implements PriorityInput {
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .registerModule(new Jdk8Module());
  private int defaultPriority;
  private BasicType schema;
  private int ttl = 15;
  private String url;
  private KeyValueCache<String, Optional<Integer>> values;

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    try {
      return values.get(MAPPER.writeValueAsString(input)).orElse(defaultPriority);
    } catch (JacksonException e) {
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

  public String getUrl() {
    return url;
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

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public void startup(String resourceName, String inputName) {
    values =
        new KeyValueCache<>(resourceName + " " + inputName, ttl, SimpleRecord::new) {
          @Override
          protected Optional<Integer> fetch(String body, Instant instant) throws Exception {
            return HTTP_CLIENT
                .send(
                    HttpRequest.newBuilder(URI.create(url))
                        .header("Content-type", "application/json")
                        .POST(BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                        .build(),
                    new JsonBodyHandler<>(MAPPER, new TypeReference<Optional<Integer>>() {}))
                .body()
                .get();
          }
        };
  }
}
