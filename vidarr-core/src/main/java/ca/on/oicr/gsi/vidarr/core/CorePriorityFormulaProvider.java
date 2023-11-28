package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.PriorityFormula;
import ca.on.oicr.gsi.vidarr.PriorityFormulaProvider;
import java.util.stream.Stream;

public final class CorePriorityFormulaProvider implements PriorityFormulaProvider {

  @Override
  public Stream<Pair<String, Class<? extends PriorityFormula>>> types() {
    return Stream.of(
        new Pair<>("constant", ConstantPriorityFormula.class),
        new Pair<>("difference", SubtractPriorityFormula.class),
        new Pair<>("escalating-multiplier", MultiplierTemporalEscalationPriorityFormula.class),
        new Pair<>("escalating-offset", OffsetTemporalEscalationPriorityFormula.class),
        new Pair<>("input", PriorityFormulaVariable.class),
        new Pair<>("maximum", MaximumPriorityFormula.class),
        new Pair<>("minimum", MinimumPriorityFormula.class),
        new Pair<>("product", ProductPriorityFormula.class),
        new Pair<>("sum", SumPriorityFormula.class));
  }
}
