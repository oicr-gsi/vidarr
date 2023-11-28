package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityFormula;
import java.time.Instant;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

public abstract class BaseAggregatePriorityFormula implements PriorityFormula {
  private List<PriorityFormula> components;

  protected abstract int aggregate(IntStream stream);

  @Override
  public final int compute(ToIntFunction<String> inputs, Instant created) {
    return aggregate(components.stream().mapToInt(c -> c.compute(inputs, created)));
  }

  public final List<PriorityFormula> getComponents() {
    return components;
  }

  public final void setComponents(List<PriorityFormula> components) {
    this.components = components;
  }
}
