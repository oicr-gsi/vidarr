package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityFormula;
import java.time.Instant;
import java.util.function.ToIntFunction;

public final class ConstantPriorityFormula implements PriorityFormula {
  private int value;

  @Override
  public int compute(ToIntFunction<String> inputs, Instant created) {
    return value;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }
}
