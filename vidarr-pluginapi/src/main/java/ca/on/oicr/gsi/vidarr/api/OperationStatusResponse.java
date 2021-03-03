package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OperationStatusResponse {

  private int attempt;
  private JsonNode debugInformation;
  private String enginePhase;
  private JsonNode recoveryState;
  private String status;
  private String type;

  public int getAttempt() {
    return attempt;
  }

  public JsonNode getDebugInformation() {
    return debugInformation;
  }

  public String getEnginePhase() {
    return enginePhase;
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

  public void setAttempt(int attempt) {
    this.attempt = attempt;
  }

  public void setDebugInformation(JsonNode debugInformation) {
    this.debugInformation = debugInformation;
  }

  public void setEnginePhase(String enginePhase) {
    this.enginePhase = enginePhase;
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
