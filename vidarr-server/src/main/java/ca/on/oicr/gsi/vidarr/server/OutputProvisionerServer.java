package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;
import java.io.IOException;
import javax.xml.stream.XMLStreamException;

public class OutputProvisionerServer
    extends BasePluginServer<OutputProvisionerProvider, OutputProvisioner> {

  public OutputProvisionerServer(String[] args) throws IOException {
    super(OutputProvisionerProvider.class, args);
  }

  @Override
  protected void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    instance.configuration(sectionRenderer);
  }

  @Override
  protected HttpHandler handlers(RoutingHandler routingHandler) {
    // TODO: actually provide an interface
    return routingHandler;
  }

  @Override
  protected OutputProvisioner readConfiguration(
      OutputProvisionerProvider provider, ObjectNode configuration) {
    return provider.readConfiguration(configuration);
  }

  @Override
  protected String typeFor(OutputProvisionerProvider pluginProvider) {
    return pluginProvider.type();
  }
}
