package ca.on.oicr.gsi.vidarr.core;

import java.util.stream.IntStream;

public final class ProductPriorityFormula extends BaseAggregatePriorityFormula {

  @Override
  protected int aggregate(IntStream stream) {
    return stream.reduce(1, (x, y) -> x * y);
  }
}
