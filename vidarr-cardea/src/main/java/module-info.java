import ca.on.oicr.gsi.vidarr.PriorityInputProvider;
import ca.on.oicr.gsi.vidarr.cardea.CardeaCasePriorityInputProvider;

module ca.on.oicr.gsi.vidarr.cardea {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires tools.jackson.core;
  requires tools.jackson.databind;
  requires java.net.http;
  requires simpleclient;

  opens ca.on.oicr.gsi.vidarr.cardea to
      tools.jackson.core,
      tools.jackson.databind;

  provides PriorityInputProvider with
      CardeaCasePriorityInputProvider;
}
