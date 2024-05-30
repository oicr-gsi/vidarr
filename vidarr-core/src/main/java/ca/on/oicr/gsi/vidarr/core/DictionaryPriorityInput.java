package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.JsonPost;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public final class DictionaryPriorityInput implements PriorityInput {
  // Jdk8Module is a compatibility fix for de/serializing Optionals
  private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
  private int defaultPriority;
  private Path file;
  private Map<String, Integer> values = Map.of();

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    return values.getOrDefault(input.asText(), defaultPriority);
  }

  private void dump(HttpServerExchange exchange) throws IOException {
    exchange.setStatusCode(StatusCodes.OK);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
    exchange.getResponseSender().send(MAPPER.writeValueAsString(values));
  }

  public int getDefaultPriority() {
    return defaultPriority;
  }

  public Path getFile() {
    return file;
  }

  @Override
  public Optional<HttpHandler> httpHandler() {
    return Optional.of(
        Handlers.routing()
            .get("/", this::dump)
            .put("/", JsonPost.parse(MAPPER, new TypeReference<>() {}, this::update)));
  }

  @Override
  public BasicType inputFromSubmitter() {
    return BasicType.STRING;
  }

  public void setDefaultPriority(int defaultPriority) {
    this.defaultPriority = defaultPriority;
  }

  public void setFile(Path file) {
    this.file = file;
  }

  @Override
  public void startup(String resourceName, String inputName) {
    try {
      values = MAPPER.readValue(file.toFile(), new TypeReference<>() {});
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void update(HttpServerExchange exchange, Map<String, Integer> newValues)
      throws IOException {
    values = newValues;
    MAPPER.writeValue(file.toFile(), values);
    exchange.setStatusCode(StatusCodes.OK);
    exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
    exchange.getResponseSender().send("");
  }
}
