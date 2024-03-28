package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.ActiveOperation;
import ca.on.oicr.gsi.vidarr.OperationStatus;
import com.fasterxml.jackson.databind.JsonNode;
import java.lang.System.Logger.Level;

final class SingleShotOperation implements ActiveOperation<Void> {
  private JsonNode state;
  private final SingleShotWorkflow singleShotWorkflow;
  private OperationStatus status = OperationStatus.INITIALIZING;
  private String type;

  public SingleShotOperation(JsonNode state, SingleShotWorkflow singleShotWorkflow) {
    this.state = state;
    this.singleShotWorkflow = singleShotWorkflow;
  }

  @Override
  public void debugInfo(JsonNode info, Void transaction) {
    // Throw this information away since we don't really want to log it
  }

  @Override
  public void error(String reason, Void transaction) {
    singleShotWorkflow.log(Level.ERROR, reason);
  }

  @Override
  public boolean isLive() {
    return true;
  }

  @Override
  public void log(System.Logger.Level level, String message) {
    singleShotWorkflow.log(level, message);
  }

  @Override
  public JsonNode recoveryState() {
    return state;
  }

  @Override
  public void recoveryState(JsonNode state, Void transaction) {
    this.state = state;
  }

  @Override
  public void status(OperationStatus status, Void transaction) {
    this.status = status;
    if (status == OperationStatus.FAILED) {
      singleShotWorkflow.fail();
    }
  }

  @Override
  public OperationStatus status() {
    return status;
  }

  @Override
  public String type() {
    return type;
  }

  @Override
  public void type(String type, Void transaction) {
    this.type = type;
  }
}
