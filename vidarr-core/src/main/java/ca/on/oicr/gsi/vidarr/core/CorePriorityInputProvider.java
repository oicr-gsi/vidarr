package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import ca.on.oicr.gsi.vidarr.PriorityInputProvider;
import java.util.stream.Stream;

public final class CorePriorityInputProvider implements PriorityInputProvider {

  @Override
  public Stream<Pair<String, Class<? extends PriorityInput>>> types() {
    return Stream.of(
        new Pair<>("json-array", ArrayPriorityInput.class),
        new Pair<>("json-dictionary", DictionaryPriorityInput.class),
        new Pair<>("oneOf", OneOfPriorityInput.class),
        new Pair<>("raw", RawPriorityInput.class),
        new Pair<>("remote", RemotePriorityInput.class),
        new Pair<>("tuple", TuplePriorityInput.class));
  }
}
