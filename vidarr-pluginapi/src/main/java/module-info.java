/**
 * The API plugins are expected to implement
 *
 * <p>Vidarr uses plugins to communicate with external services. This module defines the APIs that
 * plugins are expected to provide as well as accessory data required or provided.
 */
module ca.on.oicr.gsi.vidarr.pluginapi {
  exports ca.on.oicr.gsi.vidarr;

  requires transitive com.fasterxml.jackson.core;
  requires transitive com.fasterxml.jackson.databind;
  requires transitive server.utils;
  requires java.xml;
  requires java.net.http;
}
