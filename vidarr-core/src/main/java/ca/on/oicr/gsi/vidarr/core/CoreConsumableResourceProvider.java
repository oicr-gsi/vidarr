package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import java.util.stream.Stream;

public final class CoreConsumableResourceProvider implements ConsumableResourceProvider {

  @Override
  public Stream<Pair<String, Class<? extends ConsumableResource>>> types() {
    return Stream.of(
        new Pair<>("manual-override", ManualOverrideConsumableResource.class),
        new Pair<>("max-in-flight", MaxInFlightConsumableResource.class),
        new Pair<>("priority", PriorityConsumableResource.class));
  }
}
