package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import java.util.stream.Stream;

/** Reads JSON configuration and instantiates input provisioners appropriately */
public interface InputProvisionerProvider {
  /** Provides the type names and classes this plugin provides */
  Stream<Pair<String, Class<? extends InputProvisioner>>> types();
}
