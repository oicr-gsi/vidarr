package ca.on.oicr.gsi.vidarr.core;

import java.util.stream.IntStream;

public final class MaximumPriorityFormula extends BaseAggregatePriorityFormula {

  @Override
  protected int aggregate(IntStream stream) {
    return stream.max().orElse(Integer.MIN_VALUE);
  }
}
