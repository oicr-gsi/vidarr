package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Optional;

public final class TuplePriorityInput implements PriorityInput {

  private PriorityInput inner;

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    return inner.compute(
        workflowName,
        workflowVersion,
        created,
        input.has(0) ? input.get(0) : NullNode.getInstance());
  }

  public PriorityInput getInner() {
    return inner;
  }

  @Override
  public Optional<HttpHandler> httpHandler() {
    return inner.httpHandler();
  }

  @Override
  public BasicType inputFromSubmitter() {
    return BasicType.tuple(inner.inputFromSubmitter());
  }

  public void setInner(PriorityInput inner) {
    this.inner = inner;
  }

  @Override
  public void startup(String resourceName, String inputName) {
    inner.startup(resourceName, inputName);
  }
}
