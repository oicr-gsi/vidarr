package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ReprovisionOutResponse {
  private Map<String, String> relocations;

  public Map<String, String> getRelocations() {
    return relocations;
  }

  public void setRelocations(Map<String, String> relocations) {
    this.relocations = relocations;
  }
}