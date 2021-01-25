package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OperationStatusResponse {

  private JsonNode debugInformation;
  private JsonNode recoveryState;
  private String status;
  private String type;

  public JsonNode getDebugInformation() {
    return debugInformation;
  }

  public JsonNode getRecoveryState() {
    return recoveryState;
  }

  public String getStatus() {
    return status;
  }

  public String getType() {
    return type;
  }

  public void setDebugInformation(JsonNode debugInformation) {
    this.debugInformation = debugInformation;
  }

  public void setRecoveryState(JsonNode recoveryState) {
    this.recoveryState = recoveryState;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public void setType(String type) {
    this.type = type;
  }
}
