package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.PathTemplateMatch;
import io.undertow.util.StatusCodes;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiPredicate;

public final class ManualOverrideConsumableResource implements ConsumableResource {

  // Jdk8Module is a compatibility fix for de/serializing Optionals
  private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
  private final Set<String> allowList = new TreeSet<>();
  private ConsumableResource inner;

  private void dumpAllowed(HttpServerExchange exchange) throws JsonProcessingException {
    exchange.setStatusCode(StatusCodes.OK);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
    exchange.getResponseSender().send(MAPPER.writeValueAsString(allowList));
  }

  public ConsumableResource getInner() {
    return inner;
  }

  @Override
  public Optional<HttpHandler> httpHandler() {
    final var routes =
        Handlers.routing()
            .get("/allowed", this::dumpAllowed)
            .post("/allowed/{name}", this.update(Set::add))
            .delete("/allowed/{name}", this.update(Set::remove));
    inner.httpHandler().ifPresent(routes::setFallbackHandler);
    return Optional.of(routes);
  }

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return inner.inputFromSubmitter();
  }

  @Override
  public boolean isInputFromSubmitterRequired() {
    return inner.isInputFromSubmitterRequired();
  }

  @Override
  public void recover(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Optional<JsonNode> resourceJson) {
    inner.recover(workflowName, workflowVersion, vidarrId, resourceJson);
  }

  @Override
  public void release(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {
    inner.release(workflowName, workflowVersion, vidarrId, input);
  }

  @Override
  public ConsumableResourceResponse request(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Instant createdTime,
      OptionalInt workflowMaxInFlight,
      Optional<JsonNode> input) {
    return allowList.contains(vidarrId)
        ? ConsumableResourceResponse.AVAILABLE
        : inner.request(
            workflowName, workflowVersion, vidarrId, createdTime, OptionalInt.empty(), input);
  }

  public void setInner(ConsumableResource inner) {
    this.inner = inner;
  }

  @Override
  public void startup(String name) {
    inner.startup(name);
  }

  private HttpHandler update(BiPredicate<Set<String>, String> operation) {
    return (exchange) -> {
      final var vidarrId =
          exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY).getParameters().get("name");
      exchange.setStatusCode(
          operation.test(allowList, vidarrId) ? StatusCodes.OK : StatusCodes.ALREADY_REPORTED);
      exchange.getResponseHeaders().put(Headers.CONTENT_LENGTH, 0);
      exchange.getResponseSender().send("");
    };
  }
}
