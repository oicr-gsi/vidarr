package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "result")
@JsonSubTypes({
  @JsonSubTypes.Type(value = SubmitWorkflowResponseConflict.class, name = "conflict"),
      @JsonSubTypes.Type(value = SubmitWorkflowResponseDryRun.class, name = "dry-run"),
  @JsonSubTypes.Type(value = SubmitWorkflowResponseFailure.class, name = "failure"),
      @JsonSubTypes.Type(
          value = SubmitWorkflowResponseMissingKeyVersions.class,
          name = "missing-key-version"),
  @JsonSubTypes.Type(value = SubmitWorkflowResponseSuccess.class, name = "success")
})
public abstract sealed class SubmitWorkflowResponse
    permits SubmitWorkflowResponseConflict,
        SubmitWorkflowResponseDryRun,
        SubmitWorkflowResponseFailure,
        SubmitWorkflowResponseMissingKeyVersions,
        SubmitWorkflowResponseSuccess {
  SubmitWorkflowResponse() {}
}
