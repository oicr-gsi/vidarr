package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.undertow.server.HttpHandler;
import io.undertow.server.RoutingHandler;
import java.io.IOException;
import javax.xml.stream.XMLStreamException;

public class WorkflowEngineServer extends BasePluginServer<WorkflowEngineProvider, WorkflowEngine> {

  public WorkflowEngineServer(String[] args) throws IOException {
    super(WorkflowEngineProvider.class, args);
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
  protected WorkflowEngine readConfiguration(
      WorkflowEngineProvider provider, ObjectNode configuration) {
    return provider.readConfiguration(configuration);
  }

  @Override
  protected String typeFor(WorkflowEngineProvider pluginProvider) {
    return pluginProvider.type();
  }
}
