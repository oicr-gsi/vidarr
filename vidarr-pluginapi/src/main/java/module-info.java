import ca.on.oicr.gsi.vidarr.UnloadFilterProvider;

/**
 * The API plugins are expected to implement
 *
 * <p>Vidarr uses plugins to communicate with external services. This module defines the APIs that
 * plugins are expected to provide as well as accessory data required or provided.
 */
module ca.on.oicr.gsi.vidarr.pluginapi {
  uses UnloadFilterProvider;

  exports ca.on.oicr.gsi.vidarr;
  exports ca.on.oicr.gsi.vidarr.api;

  requires transitive ca.on.oicr.gsi.serverutils;
  requires transitive com.fasterxml.jackson.core;
  requires transitive com.fasterxml.jackson.databind;
  requires java.xml;
  requires java.net.http;
}
