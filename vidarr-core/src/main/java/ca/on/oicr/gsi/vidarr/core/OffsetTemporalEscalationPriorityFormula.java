package ca.on.oicr.gsi.vidarr.core;

public final class OffsetTemporalEscalationPriorityFormula
    extends BaseTemporalEscalationPriorityFormula<Integer> {

  @Override
  protected int escalate(int original, Integer escalation) {
    return original + escalation;
  }
}
