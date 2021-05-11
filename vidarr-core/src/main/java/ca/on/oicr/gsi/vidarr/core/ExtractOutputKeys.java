package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class ExtractOutputKeys
    extends BaseOutputExtractor<Stream<Pair<String, String>>, Stream<Pair<String, String>>> {

  private final Set<Pair<String, String>> externalKeyIds;
  private final ObjectMapper mapper;
  private final boolean optional;

  public ExtractOutputKeys(
      ObjectMapper mapper,
      Set<Pair<String, String>> externalKeyIds,
      boolean optional,
      JsonNode metadata) {
    super(null, metadata);
    this.mapper = mapper;
    this.externalKeyIds = externalKeyIds;
    this.optional = optional;
  }

  @Override
  protected Stream<Pair<String, String>> handle(
      WorkflowOutputDataType format,
      boolean optional,
      JsonNode metadata,
      JsonNode output,
      OutputData outputData) {
    if (optional == this.optional) {
      return outputData.visit(
          new OutputDataVisitor<Stream<Pair<String, String>>>() {
            @Override
            public Stream<Pair<String, String>> all() {
              return externalKeyIds.stream();
            }

            @Override
            public Stream<Pair<String, String>> external(Stream<ExternalId> ids) {
              return ids.map(id -> new Pair<>(id.getProvider(), id.getId()));
            }

            @Override
            public Stream<Pair<String, String>> remaining() {
              return externalKeyIds.stream();
            }
          });
    } else {
      return Stream.empty();
    }
  }

  @Override
  protected ObjectMapper mapper() {
    return mapper;
  }

  @Override
  protected Stream<Pair<String, String>> mergeChildren(
      Stream<Stream<Pair<String, String>>> stream) {
    return stream.flatMap(Function.identity());
  }

  @Override
  protected Stream<Pair<String, String>> processChild(
      Map<String, Object> key, String name, OutputType type, JsonNode metadata, JsonNode output) {
    return type.apply(new ExtractOutputKeys(mapper, externalKeyIds, optional, metadata));
  }

  @Override
  public Stream<Pair<String, String>> unknown() {
    throw new UnsupportedOperationException("Cannot extract keys from unknown metadata");
  }
}
