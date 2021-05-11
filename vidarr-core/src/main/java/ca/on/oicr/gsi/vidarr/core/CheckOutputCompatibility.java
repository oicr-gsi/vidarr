package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.stream.Stream;

public class CheckOutputCompatibility
    extends BaseOutputExtractor<OutputCompatibility, OutputCompatibility> {

  private final ObjectMapper mapper;

  public CheckOutputCompatibility(ObjectMapper mapper, JsonNode metadata) {
    super(null, metadata);
    this.mapper = mapper;
  }

  @Override
  protected OutputCompatibility handle(
      WorkflowOutputDataType format,
      boolean optional,
      JsonNode metadata,
      JsonNode output,
      OutputData outputData) {
    return outputData.visit(
        new OutputDataVisitor<>() {
          @Override
          public OutputCompatibility all() {
            return OutputCompatibility.INDIFFERENT;
          }

          @Override
          public OutputCompatibility external(Stream<ExternalId> ids) {
            return optional
                ? OutputCompatibility.OPTIONAL_WITH_MANUAL
                : OutputCompatibility.INDIFFERENT;
          }

          @Override
          public OutputCompatibility remaining() {
            return optional
                ? OutputCompatibility.INDIFFERENT
                : OutputCompatibility.MANDATORY_WITH_REMAINING;
          }
        });
  }

  @Override
  protected ObjectMapper mapper() {
    return mapper;
  }

  @Override
  protected OutputCompatibility mergeChildren(Stream<OutputCompatibility> stream) {
    return stream.reduce(OutputCompatibility::worst).orElse(OutputCompatibility.INDIFFERENT);
  }

  @Override
  protected OutputCompatibility processChild(
      Map<String, Object> key, String name, OutputType type, JsonNode metadata, JsonNode output) {
    return type.apply(new CheckOutputCompatibility(mapper, metadata));
  }

  @Override
  public OutputCompatibility unknown() {
    return OutputCompatibility.BROKEN;
  }
}
