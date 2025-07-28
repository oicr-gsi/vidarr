package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityScorer;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;

public final class CutoffPriorityScorer implements PriorityScorer {

  private int cutoff;

  @Override
  public boolean compute(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Instant created,
      OptionalInt workflowMaxInFlight,
      int score) {
    return score > cutoff;
  }

  public int getCutoff() {
    return cutoff;
  }

  @Override
  public Optional<HttpHandler> httpHandler() {
    return Optional.empty();
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId) {
  }

  @Override
  public void complete(String workflowName, String workflowVersion, String vidarrId) {
  }

  @Override
  public void putItBack(String workflowName, String workflowVersion, String vidarrId) {
  }

  public void setCutoff(int cutoff) {
    this.cutoff = cutoff;
  }

  @Override
  public void startup() {
  }
}
