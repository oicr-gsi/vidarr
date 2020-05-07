package ca.on.oicr.gsi.vidarr.server.remote;

import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RemoteWorkflowEngineProvider implements WorkflowEngineProvider {

  @Override
  public WorkflowEngine readConfiguration(ObjectNode node) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String type() {
    return "remote";
  }
}
