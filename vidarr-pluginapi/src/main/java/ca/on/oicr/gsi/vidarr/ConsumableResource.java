package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;

/** A broker that can block workflows from starting by managing their resource footprints */
public interface ConsumableResource {

  /**
   * To operate, this resource requires the submission request to include a parameter.
   *
   * @return the name of the parameter and the type required or an empty value if no input is
   *     required.
   */
  Optional<Pair<String, BasicType>> inputFromUser();

  /** The name provided during initialisation */
  String name();
  /**
   * Indicate that Vidarr has restarted and it is reasserting an old claim.
   *
   * @param workflowName the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId the identifier of the workflow run
   */
  void recover(String workflowName, String workflowVersion, String vidarrId);

  /**
   * Indicate that the workflow is done with this resource
   *
   * @param workflowName the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId the identifier of the workflow run
   */
  void release(String workflowName, String workflowVersion, String vidarrId);

  /**
   * Request the resource
   *
   * @param workflowName the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId the identifier of the workflow run
   * @param input the input requested from the submitter, if applicable and provided
   * @return whether this resource is available
   */
  ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input);
}
