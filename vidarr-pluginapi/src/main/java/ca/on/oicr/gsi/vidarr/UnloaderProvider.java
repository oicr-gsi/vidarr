package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** A target that stores unloaded data */
public interface UnloaderProvider {

  /**
   * Start an unload process
   *
   * @param parameters the parameters provided by the user required by the provider
   * @return an unloader to manage the unload process
   */
  Unloader create(ObjectNode parameters);

  /** The user-facing name for this provider */
  String name();
}
