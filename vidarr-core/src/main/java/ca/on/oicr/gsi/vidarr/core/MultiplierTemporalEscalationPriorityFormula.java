package ca.on.oicr.gsi.vidarr.core;

public final class MultiplierTemporalEscalationPriorityFormula
    extends BaseTemporalEscalationPriorityFormula<Double> {

  @Override
  protected int escalate(int original, Double escalation) {
    final var result = original * escalation;
    return result > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) (result);
  }
}
