package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OutputProvisionType;
import ca.on.oicr.gsi.vidarr.OutputProvisionType.IdentifierKey;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Maraud over output metadata
 *
 * @param <R> the return type
 * @param <E> the return type for an entry in a list
 */
abstract class BaseOutputExtractor<R, E> implements OutputProvisionType.Visitor<R> {

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
      switch (metadata.get("type").asText()) {
        case "REMAINING":
          return handle(
              format,
              contents,
              output,
              new OutputData() {
                @Override
                public <R> R visit(OutputDataVisitor<R> visitor) {
                  return visitor.remaining();
                }
              });
        case "ALL":
          return handle(
              format,
              contents,
              output,
              new OutputData() {
                @Override
                public <R> R visit(OutputDataVisitor<R> visitor) {
                  return visitor.all();
                }
              });
        case "MANUAL":
          try {
            if (!metadata.has(OutputProvisionType.MANUAL_FIELD__EXTERNAL_IDS)) {
              return invalid();
            }
            var externalIds =
                mapper()
                    .treeToValue(
                        metadata.get(OutputProvisionType.MANUAL_FIELD__EXTERNAL_IDS),
                        ExternalId[].class);
            return handle(
                format,
                contents,
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
  public final R list(Map<String, IdentifierKey> keys, Map<String, OutputProvisionType> outputs) {
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
          final var outputValues = new ArrayList<E>();
          if (mapping == null) {
            return invalid();
          }
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
          return mergeChildren(outputValues.stream());
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
      Map<String, Object> key, OutputProvisionType type, JsonNode metadata, JsonNode output);

  @Override
  public final R qualityControl() {
    return handle(WorkflowOutputDataType.QUALITY_CONTROL);
  }

  @Override
  public final R taggedUnion(Stream<Entry<String, OutputProvisionType>> elements) {
    if (metadata.isObject() && metadata.has("type") && metadata.has("contents")) {
      final var type = metadata.get("type").asText("");
      return elements
          .filter(e -> e.getKey().equals(type))
          .findFirst()
          .map(e -> taggedUnion(e.getValue(), metadata.get("contents")))
          .orElseGet(this::unknown);

    } else {
      return invalid();
    }
  }

  protected abstract R taggedUnion(OutputProvisionType type, JsonNode metadata);

  @Override
  public final R warehouseRecords() {
    return handle(WorkflowOutputDataType.DATAWAREHOUSE_RECORDS);
  }
}
