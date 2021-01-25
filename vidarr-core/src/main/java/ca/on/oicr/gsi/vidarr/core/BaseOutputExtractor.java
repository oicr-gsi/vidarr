package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.OutputType.IdentifierKey;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.stream.Stream;

/**
 * Maraud over output metadata
 *
 * @param <R> the return type
 * @param <E> the return type for an entry in a list
 */
abstract class BaseOutputExtractor<R, E> implements OutputType.Visitor<R> {

  public interface OutputData {
    <T> T visit(OutputDataVisitor<T> visitor);
  }

  public interface OutputDataVisitor<T> {
    T all();

    T external(Stream<ExternalId> ids);

    T remaining();
  }

  protected final JsonNode metadata;
  protected final JsonNode output;

  public BaseOutputExtractor(JsonNode output, JsonNode metadata) {
    this.output = output;
    this.metadata = metadata;
  }

  @Override
  public final R file() {
    return handle(WorkflowOutputDataType.FILE);
  }

  @Override
  public final R fileWithLabels() {
    return handle(WorkflowOutputDataType.FILE_WITH_LABELS);
  }

  @Override
  public final R files() {
    return handle(WorkflowOutputDataType.FILES);
  }

  @Override
  public final R filesWithLabels() {
    return handle(WorkflowOutputDataType.FILES_WITH_LABELS);
  }

  private R handle(WorkflowOutputDataType format) {

    if (metadata.isObject()
        && metadata.has("type")
        && metadata.get("type").isTextual()
        && metadata.has("contents")) {
      final var contents = metadata.get("contents");
      if (!contents.isArray()) {
        return invalid();
      }
      switch (metadata.get("type").asText()) {
        case "REMAINING":
          if (contents.size() != 1) {
            return invalid();
          }
          return handle(
              format,
              contents.get(0),
              output,
              new OutputData() {
                @Override
                public <R> R visit(OutputDataVisitor<R> visitor) {
                  return visitor.remaining();
                }
              });
        case "ALL":
          if (contents.size() != 1) {
            return invalid();
          }
          return handle(
              format,
              contents.get(0),
              output,
              new OutputData() {
                @Override
                public <R> R visit(OutputDataVisitor<R> visitor) {
                  return visitor.all();
                }
              });
        case "MANUAL":
          try {
            if (contents.size() != 2) {
              return invalid();
            }
            var externalIds = mapper().treeToValue(contents.get(1), ExternalId[].class);
            return handle(
                format,
                contents.get(0),
                output,
                new OutputData() {
                  @Override
                  public <R> R visit(OutputDataVisitor<R> visitor) {
                    return visitor.external(Stream.of(externalIds));
                  }
                });

          } catch (Exception e) {
            return invalid();
          }
        default:
          return invalid();
      }
    } else {
      return invalid();
    }
  }

  protected abstract R handle(
      WorkflowOutputDataType format, JsonNode metadata, JsonNode output, OutputData outputData);

  protected R invalid() {
    throw new IllegalArgumentException();
  }

  @Override
  public final R list(Map<String, IdentifierKey> keys, Map<String, OutputType> outputs) {
    if (metadata.isArray() && (output == null || output.isArray())) {
      final var outputMappings = new HashMap<Map<String, Object>, Map<String, JsonNode>>();
      for (final var child : metadata) {
        if (!child.isObject()) {
          return invalid();
        }
        final var key = new TreeMap<String, Object>();
        for (final var identifier : keys.entrySet()) {
          key.put(
              identifier.getKey(),
              switch (identifier.getValue()) {
                case INTEGER -> child.get(identifier.getKey()).asLong();
                case STRING -> child.get(identifier.getKey()).asText();
              });
        }
        final var value = new TreeMap<String, JsonNode>();
        for (final var output : outputs.entrySet()) {
          value.put(output.getKey(), metadata.get(output.getKey()));
        }
        outputMappings.put(key, value);
      }
      if (output == null) {
        final var outputValues = new ArrayList<E>();
        for (final var mapping : outputMappings.entrySet()) {
          for (final var output : mapping.getValue().entrySet()) {
            outputValues.add(
                processChild(
                    mapping.getKey(), outputs.get(output.getKey()), output.getValue(), null));
          }
        }
        return mergeChildren(outputValues.stream());
      } else {
        final var outputValues = new ArrayList<E>();
        final var unusedKeys = new HashSet<>(outputMappings.keySet());
        if (output.isEmpty()) {
          return invalid();
        }
        for (final var child : output) {
          if (!child.isObject()) {
            return invalid();
          }
          final var key = new TreeMap<String, Object>();
          for (final var identifier : keys.entrySet()) {
            key.put(
                identifier.getKey(),
                switch (identifier.getValue()) {
                  case INTEGER -> child.get(identifier.getKey()).asLong();
                  case STRING -> child.get(identifier.getKey()).asText();
                });
          }
          final var mapping = outputMappings.get(key);
          if (mapping == null) {
            return invalid();
          }
          unusedKeys.remove(key);
          for (final var output : outputs.entrySet()) {
            if (!child.has(output.getKey())) {
              return invalid();
            }
            outputValues.add(
                processChild(
                    key,
                    output.getValue(),
                    mapping.get(output.getKey()),
                    child.get(output.getKey())));
          }
          if (unusedKeys.isEmpty()) {
            return mergeChildren(outputValues.stream());
          } else {
            return invalid();
          }
        }
      }
      return invalid();
    } else {
      return invalid();
    }
  }

  @Override
  public final R logs() {
    return handle(WorkflowOutputDataType.LOGS);
  }

  protected abstract ObjectMapper mapper();

  protected abstract R mergeChildren(Stream<E> stream);

  protected abstract E processChild(
      Map<String, Object> key, OutputType type, JsonNode metadata, JsonNode output);

  @Override
  public final R qualityControl() {
    return handle(WorkflowOutputDataType.QUALITY_CONTROL);
  }

  @Override
  public final R warehouseRecords() {
    return handle(WorkflowOutputDataType.DATAWAREHOUSE_RECORDS);
  }
}
