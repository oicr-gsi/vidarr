package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import java.util.stream.Stream;

/** Defines JSON objects that can be used as workflow engines */
public interface WorkflowEngineProvider {

  /** Provides the type names and classes this plugin provides */
  Stream<Pair<String, Class<? extends WorkflowEngine>>> types();
}
