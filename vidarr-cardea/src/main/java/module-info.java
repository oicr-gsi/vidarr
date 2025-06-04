import ca.on.oicr.gsi.vidarr.PriorityInputProvider;
import ca.on.oicr.gsi.vidarr.cardea.CardeaCasePriorityInputProvider;

module ca.on.oicr.gsi.vidarr.cardea {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires com.fasterxml.jackson.datatype.jdk8;
  requires java.net.http;
  requires simpleclient;

  opens ca.on.oicr.gsi.vidarr.cardea to
      com.fasterxml.jackson.core,
      com.fasterxml.jackson.databind;

  provides PriorityInputProvider with
      CardeaCasePriorityInputProvider;
}
