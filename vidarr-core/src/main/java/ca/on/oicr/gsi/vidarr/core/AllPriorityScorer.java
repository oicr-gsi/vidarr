package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityScorer;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
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
    // Compute score for all of the scorers so they get a chance to build their caches
    // Not optimal to exit early in this case as that would prevent all caches from being built.
    List<PriorityScorer> okayScorers = new LinkedList<>();
    for (PriorityScorer scorer : scorers) {
      if (scorer.compute(workflowName, workflowVersion, vidarrId, created, workflowMaxInFlight,
          score)) {
        okayScorers.add(scorer);
      }
    }

    // If one or more of the scorers is not OK, release the ones that are and return false
    if (okayScorers.size() != scorers.size()) {
      for (PriorityScorer scorer : okayScorers) {
        scorer.putItBack(workflowName, workflowVersion, vidarrId);
      }
      return false;
    } else { // all of the scorers returned true, we are good to go
      return true;
    }
  }

  @Override
  protected void startupInner() {
    // Do nothing
  }
}
