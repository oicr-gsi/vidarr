package ca.on.oicr.gsi.vidarr.core;

import java.time.Instant;
import java.util.OptionalInt;

public final class AnyPriorityScorer extends BaseAggregatePriorityScorer {

  @Override
  public boolean compute(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Instant created,
      OptionalInt workflowMaxInFlight,
      int score) {
    for (var i = 0; i < scorers.size(); i++) {
      if (scorers
          .get(i)
          .compute(workflowName, workflowVersion, vidarrId, created, workflowMaxInFlight, score)) {
        for (var j = 0; j < scorers.size(); j++) {
          if (j != i) {
            scorers.get(j).recover(workflowName, workflowVersion, vidarrId);
          }
        }
        return true;
      }
    }

    return false;
  }

  @Override
  protected void startupInner() {
    // Do nothing.
  }
}
