package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityFormula;
import java.time.Instant;
import java.util.function.ToIntFunction;

public final class PriorityFormulaVariable implements PriorityFormula {
  private String name;

  @Override
  public int compute(ToIntFunction<String> inputs, Instant created) {
    return inputs.applyAsInt(name);
  }

  public void setName(String name) {
    this.name = name;
  }
}
