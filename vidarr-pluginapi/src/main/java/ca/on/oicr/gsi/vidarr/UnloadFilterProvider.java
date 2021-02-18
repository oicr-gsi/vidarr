package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import java.util.stream.Stream;

/** A target that stores unloaded data */
public interface UnloadFilterProvider {
  /** The filter types that can be decoded and their JSON <tt>type</tt> property */
  Stream<Pair<String, Class<? extends UnloadFilter>>> types();
}
