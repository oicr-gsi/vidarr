package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import java.util.stream.Stream;

/** Reads JSON configuration and instantiates priority formulae appropriately */
public interface PriorityFormulaProvider {
  /** Provides the type names and classes this plugin provides */
  Stream<Pair<String, Class<? extends PriorityFormula>>> types();
}
