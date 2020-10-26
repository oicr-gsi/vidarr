package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class SubmitWorkflowResponseFailure extends SubmitWorkflowResponse {
  private List<String> errors;

  public SubmitWorkflowResponseFailure() {}

  public SubmitWorkflowResponseFailure(String error) {
    errors = List.of(error);
  }

  public SubmitWorkflowResponseFailure(Collection<String> errors) {
    this.errors = new ArrayList<>(errors);
  }

  public List<String> getErrors() {
    return errors;
  }

  public void setErrors(List<String> errors) {
    this.errors = errors;
  }
}
