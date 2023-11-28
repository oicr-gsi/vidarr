package ca.on.oicr.gsi.vidarr.core;

import java.util.stream.IntStream;

public final class SumPriorityFormula extends BaseAggregatePriorityFormula {

  @Override
  protected int aggregate(IntStream stream) {
    return stream.sum();
  }
}
