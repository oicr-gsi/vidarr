package ca.on.oicr.gsi.vidarr.cli;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;

public final class TestCase {
  private ObjectNode arguments;
  private String description;
  private ObjectNode engineArguments;
  private String id;
  private ObjectNode metadata;
  private List<TestValidator> validators;

  public ObjectNode getArguments() {
    return arguments;
  }

  public String getDescription() {
    return description;
  }

  public ObjectNode getEngineArguments() {
    return engineArguments;
  }

  public String getId() {
    return id;
  }

  public ObjectNode getMetadata() {
    return metadata;
  }

  public List<TestValidator> getValidators() {
    return validators;
  }

  public void setArguments(ObjectNode arguments) {
    this.arguments = arguments;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setEngineArguments(ObjectNode engineArguments) {
    this.engineArguments = engineArguments;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setMetadata(ObjectNode metadata) {
    this.metadata = metadata;
  }

  public void setValidators(List<TestValidator> validators) {
    this.validators = validators;
  }
}
