package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;

public final class CromwellWorkflowEngine implements WorkflowEngine {
  public static WorkflowEngineProvider provider() {
    return new WorkflowEngineProvider() {
      @Override
      public Optional<WorkflowEngine> readConfiguration(ObjectNode node) {
        // TODO
        return Optional.empty();
      }

      @Override
      public String type() {
        return "cromwell";
      }
    };
  }
}
