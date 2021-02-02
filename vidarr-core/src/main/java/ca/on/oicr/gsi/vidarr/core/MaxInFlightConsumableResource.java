package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class MaxInFlightConsumableResource implements ConsumableResource {
  public static ConsumableResourceProvider provider() {
    return new ConsumableResourceProvider() {
      @Override
      public ConsumableResource readConfiguration(String name, ObjectNode node) {
        return new MaxInFlightConsumableResource(name, node.get("maximum").asInt(0));
      }

      @Override
      public String type() {
        return "max-in-flight";
      }
    };
  }

  private final String name;
  private final int maximum;
  private final Set<String> inFlight;

  public MaxInFlightConsumableResource(String name, int maximum) {
    this.name = name;
    this.maximum = maximum;
    inFlight = ConcurrentHashMap.newKeySet(maximum);
  }

  @Override
  public Optional<Pair<String, BasicType>> inputFromUser() {
    return Optional.empty();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId) {
    inFlight.add(vidarrId);
  }

  @Override
  public void release(String workflowName, String workflowVersion, String vidarrId) {
    inFlight.remove(vidarrId);
  }

  @Override
  public ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {
    if (inFlight.size() <= maximum) {
      return ConsumableResourceResponse.AVAILABLE;
    } else {
      return ConsumableResourceResponse.UNAVAILABLE;
    }
  }
}
