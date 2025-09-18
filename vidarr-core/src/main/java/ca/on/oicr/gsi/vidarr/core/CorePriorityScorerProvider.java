package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.PriorityScorer;
import ca.on.oicr.gsi.vidarr.PriorityScorerProvider;
import java.util.stream.Stream;

public class CorePriorityScorerProvider implements PriorityScorerProvider {

  @Override
  public Stream<Pair<String, Class<? extends PriorityScorer>>> types() {
    return Stream.of(
        new Pair<>("all", AllPriorityScorer.class),
        new Pair<>("any", AnyPriorityScorer.class),
        new Pair<>("cutoff", CutoffPriorityScorer.class),
        new Pair<>("ranked-max-in-flight", InFlightCollectingPriorityScorer.class),
        new Pair<>(
            "ranked-max-in-flight-by-workflow", InFlightCollectingByWorkflowPriorityScorer.class),
        new Pair<>(
            "ranked-max-in-flight-by-workflow-version",
            InFlightCollectingByWorkflowVersionPriorityScorer.class),
        new Pair<>(
            "resource-optimizing", ResourceOptimizingPriorityScorer.class)
    );
  }
}
