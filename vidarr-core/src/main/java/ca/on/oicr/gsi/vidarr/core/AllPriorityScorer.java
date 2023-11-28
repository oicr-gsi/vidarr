package ca.on.oicr.gsi.vidarr.core;

import java.time.Instant;
import java.util.OptionalInt;

public final class AllPriorityScorer extends BaseAggregatePriorityScorer {

  @Override
  public boolean compute(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Instant created,
      OptionalInt workflowMaxInFlight,
      int score) {
    for (var i = 0; i < scorers.size(); i++) {
      if (!scorers
          .get(i)
          .compute(workflowName, workflowVersion, vidarrId, created, workflowMaxInFlight, score)) {
        for (var j = 0; j < i; j++) {
          scorers.get(j).release(workflowName, workflowVersion, vidarrId);
        }
        return false;
      }
    }

    return true;
  }

  @Override
  protected void startupInner() {
    // Do nothing
  }
}
