package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashSet;

import static ca.on.oicr.gsi.vidarr.niassa.NiassaWorkflowEngine.MAPPER;

public class NiassaWorkflowEngineProvider implements WorkflowEngineProvider {
    @Override
    public WorkflowEngine readConfiguration(ObjectNode node) {
        return new NiassaWorkflowEngine(node.get("dbUrl").asText(),
                node.get("dbUser").asText(),
                node.get("dbPass").asText(),
                // VERY unclear whether this will work
                node.has("annotations")?
                        MAPPER.convertValue(node.get("annotations"), new TypeReference<>() {})
                        : new HashSet<>());
    }

    @Override
    public String type() {
        return "niassa";
    }
}
