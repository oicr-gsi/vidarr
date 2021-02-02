package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.BasicType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AddWorkflowRequest {
  private Map<String, BasicType> labels = Collections.emptyMap();
  private int maxInFlight;

  public Map<String, BasicType> getLabels() {
    return labels;
  }

  public int getMaxInFlight() {
    return maxInFlight;
  }

  public void setLabels(Map<String, BasicType> labels) {
    this.labels = labels;
  }

  public void setMaxInFlight(int maxInFlight) {
    this.maxInFlight = maxInFlight;
  }
}
