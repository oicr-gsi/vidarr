package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OutputType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Check that the output metadata provided by the caller matches the workflow definition
 *
 * <p>The result will be a stream of errors; if empty, no errors were found.
 */
public final class CheckOutputType extends BaseOutputExtractor<Stream<String>, Stream<String>> {
  private final ObjectMapper mapper;
  private final Target target;

  public CheckOutputType(ObjectMapper mapper, Target target, JsonNode metadata) {
    super(null, metadata);
    this.mapper = mapper;
    this.target = target;
  }

  @Override
  protected Stream<String> handle(
      WorkflowOutputDataType format, JsonNode metadata, JsonNode output, OutputData outputData) {
    final var provision = target.provisionerFor(format.format());
    if (provision == null) {
      return Stream.of("Cannot provision output format " + format + " in this configuration.");
    } else {
      return provision.typeFor(format.format()).apply(new CheckEngineType(metadata));
    }
  }

  @Override
  protected ObjectMapper mapper() {
    return mapper;
  }

  @Override
  protected Stream<String> mergeChildren(Stream<Stream<String>> stream) {
    return stream.flatMap(Function.identity());
  }

  @Override
  protected Stream<String> processChild(
      Map<String, Object> key, OutputType type, JsonNode metadata, JsonNode output) {
    return type.apply(new CheckOutputType(mapper, target, metadata));
  }

  @Override
  public Stream<String> unknown() {
    return Stream.of("Unknown data type");
  }
}
