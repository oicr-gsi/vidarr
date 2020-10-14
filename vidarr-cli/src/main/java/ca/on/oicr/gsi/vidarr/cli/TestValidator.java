package ca.on.oicr.gsi.vidarr.cli;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {@JsonSubTypes.Type(name = "script", value = TestValidatorScript.class)})
public abstract class TestValidator {

  abstract Validator createValidator();
}
