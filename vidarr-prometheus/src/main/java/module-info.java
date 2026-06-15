import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.PriorityInputProvider;
import ca.on.oicr.gsi.vidarr.prometheus.AlertmanagerAutoInhibitConsumableResource;
import ca.on.oicr.gsi.vidarr.prometheus.PrometheusPriorityInput;

module ca.on.oicr.gsi.vidarr.prometheus {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires tools.jackson.core;
  requires tools.jackson.databind;
  requires java.net.http;

  opens ca.on.oicr.gsi.vidarr.prometheus to
      tools.jackson.core,
      tools.jackson.databind;

  provides ConsumableResourceProvider with
      AlertmanagerAutoInhibitConsumableResource;
  provides PriorityInputProvider with
      PrometheusPriorityInput;
}
