package ca.on.oicr.gsi.vidarr;

import java.util.Optional;
import java.util.stream.Stream;

/** A collection of pre-processed workflows that can be used by plugins */
public interface WorkflowRepository {
  /** Dump all workflows in the repository */
  Stream<WorkflowDefinition> fetchAll();

  /**
   * Get a specific workflow by ID
   *
   * @param id the unique has ID assigned to the workflow
   */
  Optional<WorkflowDefinition> get(String id);
}
