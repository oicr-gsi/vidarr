package ca.on.oicr.gsi.vidarr.server.dto;

import ca.on.oicr.gsi.vidarr.SimpleType;
import java.util.Collections;
import java.util.Map;

public class AddWorkflowRequest {
  private Map<String, Long> consumableResources = Collections.emptyMap();
  private Map<String, SimpleType> labels = Collections.emptyMap();
  private int maxInFlight;

  public Map<String, Long> getConsumableResources() {
    return consumableResources;
  }

  public Map<String, SimpleType> getLabels() {
    return labels;
  }

  public int getMaxInFlight() {
    return maxInFlight;
  }

  public void setConsumableResources(Map<String, Long> consumableResources) {
    this.consumableResources = consumableResources;
  }

  public void setLabels(Map<String, SimpleType> labels) {
    this.labels = labels;
  }

  public void setMaxInFlight(int maxInFlight) {
    this.maxInFlight = maxInFlight;
  }
}
