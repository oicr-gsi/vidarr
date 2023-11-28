package ca.on.oicr.gsi.vidarr.core;

import java.util.stream.IntStream;

public final class MinimumPriorityFormula extends BaseAggregatePriorityFormula {

  @Override
  protected int aggregate(IntStream stream) {
    return stream.min().orElse(Integer.MAX_VALUE);
  }
}
