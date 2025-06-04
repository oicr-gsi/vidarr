package ca.on.oicr.gsi.vidarr.cardea;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import ca.on.oicr.gsi.vidarr.PriorityInputProvider;
import java.util.stream.Stream;

public class CardeaCasePriorityInputProvider implements PriorityInputProvider {
  @Override
  public Stream<Pair<String, Class<? extends PriorityInput>>> types() {
    return Stream.of(new Pair<>("cardea-case", CardeaCasePriorityInput.class));
  }
}
