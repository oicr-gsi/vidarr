package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.OutputType.IdentifierKey;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.stream.Collectors;
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
  public final R file(boolean optional) {
    return handle(WorkflowOutputDataType.FILE, optional);
  }

  @Override
  public final R fileWithLabels(boolean optional) {
    return handle(WorkflowOutputDataType.FILE_WITH_LABELS, optional);
  }

  @Override
  public final R files(boolean optional) {
    return handle(WorkflowOutputDataType.FILES, optional);
  }

  @Override
  public final R filesWithLabels(boolean optional) {
    return handle(WorkflowOutputDataType.FILES_WITH_LABELS, optional);
  }

  private R handle(WorkflowOutputDataType format, boolean optional) {
    if (metadata.isObject()
        && metadata.has("type")
        && metadata.get("type").isTextual()
        && metadata.has("contents")) {
      final var contents = metadata.get("contents");
      if (!contents.isArray()) {
        return invalid("(in metadata): 'contents' must be array.");
      }
      switch (metadata.get("type").asText()) {
        case "REMAINING":
          var remainingExpectedSize = 1;
          if (contents.size() != remainingExpectedSize) {
            return invalid(
                String.format(
                    "(in metadata): Incorrect number of values for 'contents' of 'type' "
                        + "REMAINING in %s. "
                        + "Expected %d but got %d.",
                    format, remainingExpectedSize, contents.size()));
          }
          return handle(
              format,
              optional,
              contents.get(0),
              output,
              new OutputData() {
                @Override
                public <R> R visit(OutputDataVisitor<R> visitor) {
                  return visitor.remaining();
                }
              });
        case "ALL":
          var allExpectedSize = 1;
          if (contents.size() != allExpectedSize) {
            return invalid(
                String.format(
                    "(in metadata): Incorrect number of values for 'contents' of 'type' ALL in "
                        + "%s. Expected %d but got %d.",
                    format, allExpectedSize, contents.size()));
          }
          return handle(
              format,
              optional,
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
            var manualExpectedSize = 2;
            if (contents.size() != manualExpectedSize) {
              return invalid(
                  String.format(
                      "(in metadata): Incorrect number of values for 'contents' of 'type' MANUAL "
                          + "in %s. Expected %d but got %d",
                      format, manualExpectedSize, contents.size()));
            }
            var externalIds = mapper().treeToValue(contents.get(1), ExternalId[].class);
            return handle(
                format,
                optional,
                contents.get(0),
                output,
                new OutputData() {
                  @Override
                  public <R> R visit(OutputDataVisitor<R> visitor) {
                    return visitor.external(Stream.of(externalIds));
                  }
                });

          } catch (Exception e) {
            return invalid(e.getMessage());
          }
        default:
          return invalid(
              String.format(
                  "(in metadata): Unknown 'type' %s in %s.",
                  metadata.get("type").asText(), format));
      }
    } else if (!metadata.isObject()) {
      return invalid(
          String.format(
              "(in metadata): this must be an object (because it's for output %s), but instead "
                  + "it's a %s full of %s",
              format, metadata.getNodeType(), metadata.toString()));
    } else if (!metadata.has("type") || !metadata.get("type").isTextual()) {
      return invalid("(in metadata): must contain field 'type' which has a text value.");
    } else if (!metadata.has("contents")) {
      return invalid("(in metadata): must contain field 'contents'.");
    } else {
      return invalid("(in metadata): this object is misformatted");
    }
  }

  protected abstract R handle(
      WorkflowOutputDataType format,
      boolean optional,
      JsonNode metadata,
      JsonNode output,
      OutputData outputData);

  protected R invalid(String error) {
    throw new IllegalArgumentException(error);
  }

  @Override
  public final R list(Map<String, IdentifierKey> keys, Map<String, OutputType> outputs) {
    if (metadata.isArray() && (output == null || output.isArray())) {
      final var outputMappings = new HashMap<Map<String, Object>, Map<String, JsonNode>>();
      for (final var child : metadata) {
        if (!child.isObject()) {
          return invalid("All entries in 'metadata' list are not objects");
        }
        final var key = new TreeMap<String, Object>();
        for (final var identifier : keys.entrySet()) {
          if (!child.has(identifier.getKey())) {
            return invalid(
                String.format(
                    "Identifier key '%s' missing in 'metadata' collection.", identifier.getKey()));
          }
          key.put(
              identifier.getKey(),
              switch (identifier.getValue()) {
                case INTEGER -> child.get(identifier.getKey()).asLong();
                case STRING -> child.get(identifier.getKey()).asText();
              });
        }
        final var value = new TreeMap<String, JsonNode>();
        for (final var output : outputs.entrySet()) {
          if (!child.has(output.getKey())) {
            return invalid(
                String.format(
                    "Output value '%s' missing in collection for entry identified by '%s'.",
                    output.getKey(), key));
          }
          value.put(output.getKey(), child.get(output.getKey()));
        }
        outputMappings.put(key, value);
      }
      if (output == null) {
        final var outputValues = new ArrayList<E>();
        for (final var mapping : outputMappings.entrySet()) {
          for (final var output : mapping.getValue().entrySet()) {
            outputValues.add(
                processChild(
                    mapping.getKey(),
                    output.getKey(),
                    outputs.get(output.getKey()),
                    output.getValue(),
                    null));
          }
        }
        return mergeChildren(outputValues.stream());
      } else {
        final var outputValues = new ArrayList<E>();
        final var unusedKeys = new HashSet<>(outputMappings.keySet());
        if (output.isEmpty()) {
          return invalid("No 'output' values provided");
        }
        for (final var child : output) {
          if (!child.isObject()) {
            return invalid("Element of 'output' list must be an object.");
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
            return invalid(
                key.entrySet().stream()
                    .map(e -> e.getKey() + " = " + e.getValue())
                    .collect(Collectors.joining(", ", "Missing output mapping for (", ")")));
          }
          unusedKeys.remove(key);
          for (final var output : outputs.entrySet()) {
            if (!child.has(output.getKey())) {
              return invalid("Child of 'output' is missing output key " + output.getKey());
            }
            outputValues.add(
                processChild(
                    key,
                    output.getKey(),
                    output.getValue(),
                    mapping.get(output.getKey()),
                    child.get(output.getKey())));
          }
        }
        if (unusedKeys.isEmpty()) {
          return mergeChildren(outputValues.stream());
        } else {
          return invalid(
              unusedKeys.stream()
                  .map(
                      unusedKey ->
                          unusedKey.entrySet().stream()
                              .map(e -> e.getKey() + " = " + e.getValue())
                              .collect(Collectors.joining(", ", "(", ")")))
                  .collect(Collectors.joining(", ", "Unused keys: ", "")));
        }
      }
    } else {
      return invalid("Both 'metadata' and 'output' must be arrays.");
    }
  }

  @Override
  public final R logs(boolean optional) {
    return handle(WorkflowOutputDataType.LOGS, optional);
  }

  protected abstract ObjectMapper mapper();

  protected abstract R mergeChildren(Stream<E> stream);

  protected abstract E processChild(
      Map<String, Object> key, String name, OutputType type, JsonNode metadata, JsonNode output);

  @Override
  public final R qualityControl(boolean optional) {
    return handle(WorkflowOutputDataType.QUALITY_CONTROL, optional);
  }

  @Override
  public final R warehouseRecords(boolean optional) {
    return handle(WorkflowOutputDataType.DATAWAREHOUSE_RECORDS, optional);
  }
}
