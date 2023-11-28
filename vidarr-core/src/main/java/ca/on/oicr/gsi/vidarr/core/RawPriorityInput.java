package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import com.fasterxml.jackson.databind.JsonNode;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Optional;

public final class RawPriorityInput implements PriorityInput {

  private int defaultPriority;

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    return input.asInt(defaultPriority);
  }

  public int getDefaultPriority() {
    return defaultPriority;
  }

  @Override
  public Optional<HttpHandler> httpHandler() {
    return Optional.empty();
  }

  @Override
  public BasicType inputFromSubmitter() {
    return BasicType.INTEGER.asOptional();
  }

  public void setDefaultPriority(int defaultPriority) {
    this.defaultPriority = defaultPriority;
  }

  @Override
  public void startup(String resourceName, String inputName) {
    // Do nothing.
  }
}
