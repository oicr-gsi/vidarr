package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// This is the global one. Per-Workflow is MaxInFlightByWorkflow
public final class MaxInFlightConsumableResource implements ConsumableResource {

  @JsonIgnore private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
  private int maximum;

  public MaxInFlightConsumableResource() {}

  public int getMaximum() {
    return maximum;
  }

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return Optional.empty();
  }

  @Override
  public void recover(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Optional<JsonNode> resourceJson) {
    inFlight.add(vidarrId);
  }

  @Override
  public void release(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {
    inFlight.remove(vidarrId);
  }

  @Override
  public synchronized ConsumableResourceResponse request(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Instant createdTime,
      OptionalInt workflowMaxInFlight,
      Optional<JsonNode> input) {
    if (inFlight.size() <= maximum) {
      inFlight.add(vidarrId);
      return ConsumableResourceResponse.AVAILABLE;
    } else {
      return ConsumableResourceResponse.UNAVAILABLE;
    }
  }

  @Override
  public void startup(String name) {
    if (maximum < 0) {
      throw new IllegalArgumentException("Maximum value is negative in max-in-flight resource.");
    }
  }

  public void setMaximum(int maximum) {
    this.maximum = maximum;
  }

  @Override
  public boolean isInputFromSubmitterRequired() {
    return false;
  }
}
