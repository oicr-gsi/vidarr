package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityFormula;
import java.time.Duration;
import java.time.Instant;
import java.util.TreeMap;
import java.util.function.ToIntFunction;

public abstract class BaseTemporalEscalationPriorityFormula<T> implements PriorityFormula {
  private PriorityFormula base;
  private TreeMap<Duration, T> escalation;

  @Override
  public final int compute(ToIntFunction<String> inputs, Instant created) {
    final var entry = escalation.floorEntry(Duration.between(created, Instant.now()));
    final var original = base.compute(inputs, created);
    return entry == null ? original : escalate(original, entry.getValue());
  }

  protected abstract int escalate(int original, T escalation);

  public final PriorityFormula getBase() {
    return base;
  }

  public final TreeMap<Duration, T> getEscalation() {
    return escalation;
  }

  public final void setBase(PriorityFormula base) {
    this.base = base;
  }

  public final void setEscalation(TreeMap<Duration, T> escalation) {
    this.escalation = escalation;
  }
}
