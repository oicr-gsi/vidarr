package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowResponseConflict extends SubmitWorkflowResponse {
  // TODO: potential matches – similar to current data in Shesmu? If everything gets skipped, this
  // might be useless

}
