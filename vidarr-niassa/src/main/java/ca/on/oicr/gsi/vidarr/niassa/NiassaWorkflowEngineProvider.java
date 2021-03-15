package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NiassaWorkflowEngineProvider implements WorkflowEngineProvider {
    @Override
    public WorkflowEngine readConfiguration(ObjectNode node) {
        return null;
    }

    @Override
    public String type() {
        return "niassa";
    }
}
