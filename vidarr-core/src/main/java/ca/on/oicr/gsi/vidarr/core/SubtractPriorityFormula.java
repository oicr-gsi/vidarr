package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityFormula;
import java.time.Instant;
import java.util.function.ToIntFunction;

public final class SubtractPriorityFormula implements PriorityFormula {
  private PriorityFormula left;
  private PriorityFormula right;

  @Override
  public int compute(ToIntFunction<String> inputs, Instant created) {
    return left.compute(inputs, created) - right.compute(inputs, created);
  }

  public PriorityFormula getLeft() {
    return left;
  }

  public PriorityFormula getRight() {
    return right;
  }

  public void setLeft(PriorityFormula left) {
    this.left = left;
  }

  public void setRight(PriorityFormula right) {
    this.right = right;
  }
}
