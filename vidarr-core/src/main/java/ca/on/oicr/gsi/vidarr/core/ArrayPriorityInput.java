package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.JsonPost;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public final class ArrayPriorityInput implements PriorityInput {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private Path file;
  private int overflowPriority;
  private int underflowPriority;
  private List<Integer> values = List.of();

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    // Create a local reference for thread safety
    final var values = this.values;
    if (input.isInt()) {
      final var index = input.asInt();
      if (index < 0) {
        return underflowPriority;
      } else if (index >= values.size()) {
        return overflowPriority;
      } else {
        return values.get(index);
      }
    } else {
      return underflowPriority;
    }
  }

  private void dump(HttpServerExchange exchange) throws IOException {
    exchange.setStatusCode(StatusCodes.OK);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
    exchange.getResponseSender().send(MAPPER.writeValueAsString(values));
  }

  public Path getFile() {
    return file;
  }

  public int getOverflowPriority() {
    return overflowPriority;
  }

  public int getUnderflowPriority() {
    return underflowPriority;
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

  public void setFile(Path file) {
    this.file = file;
  }

  public void setOverflowPriority(int overflowPriority) {
    this.overflowPriority = overflowPriority;
  }

  public void setUnderflowPriority(int underflowPriority) {
    this.underflowPriority = underflowPriority;
  }

  @Override
  public void startup(String resourceName, String inputName) {
    try {
      values = MAPPER.readValue(file.toFile(), new TypeReference<>() {});
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void update(HttpServerExchange exchange, List<Integer> newValues) throws IOException {
    values = newValues;
    MAPPER.writeValue(file.toFile(), values);
    exchange.setStatusCode(StatusCodes.OK);
    exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
    exchange.getResponseSender().send("");
  }
}
