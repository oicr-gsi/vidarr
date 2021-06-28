package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * The type of elements to used as outputs when provisioning out structures
 *
 * <p>When provisioning a list of items, each element in the structure needs to be converted to part
 * of a composite provisioning key or an output to be provisioned. These are the allowable types for
 * data. The submission and workflow will have asymmetric information: the submission is providing
 * metadata (<i>e.g.</i>, the LIMS keys for a file) while the workflow is providing data
 * (<i>e.g.</i> the file path) and the provision out process will marry the two.
 */
@JsonSerialize(using = OutputType.JacksonSerializer.class)
@JsonDeserialize(using = OutputType.JacksonDeserializer.class)
public abstract class OutputType {
  private static final String STR_FILE = "file",
      STR_FILE_OPTIONAL = "optional-file",
      STR_FILES = "files",
      STR_FILES_OPTIONAL = "optional-files",
      STR_FILES_WITH_LABELS = "files-with-labels",
      STR_FILES_WITH_LABELS_OPTIONAL = "optional-files-with-labels",
      STR_FILE_WITH_LABELS = "file-with-labels",
      STR_FILE_WITH_LABELS_OPTIONAL = "optional-file-with-labels",
      STR_LOGS = "logs",
      STR_LOGS_OPTIONAL = "optional-logs",
      STR_QUALITY_CONTROL = "quality-control",
      STR_QUALITY_CONTROL_OPTIONAL = "optional-quality-control",
      STR_UNKNOWN = "unknown",
      STR_WAREHOUSE_RECORDS = "warehouse-records",
      STR_WAREHOUSE_RECORDS_OPTIONAL = "optional-warehouse-records",
      STR_LIST = "list",
      STR_IS = "is",
      STR_KEYS = "keys",
      STR_OUTPUTS = "outputs";
  /**
   * The type of elements to used as keys when provisioning out structures
   *
   * <p>When provisioning a list of items, each element in the structure needs to be converted to
   * part of a composite provisioning key or an output to be provisioned. These are the allowable
   * types for keys that both the workflow and the submission request must supply
   */
  public enum IdentifierKey {
    /**
     * An integer
     *
     * <p>The size of the integer is not implied (that is, there is no distinction between
     * <tt>int</tt> and <tt>long</tt>).
     */
    INTEGER,
    /** A string */
    STRING
  }

  /**
   * A visitor to interrogate an output type structure
   *
   * @param <T> the return type of the visitor
   */
  public interface Visitor<T> {

    /** The output is a single files */
    default T file(boolean optional) {
      return unknown();
    }

    /** The output is a pair of a single files and a dictionary of labels */
    default T fileWithLabels(boolean optional) {
      return unknown();
    }

    /** The output is a list of files */
    default T files(boolean optional) {
      return unknown();
    }

    /** The output is a pair of a list of files and a dictionary of labels */
    default T filesWithLabels(boolean optional) {
      return unknown();
    }

    /**
     * The output data is a list of structures
     *
     * @param keys the key that uniquely identify entries in the structure
     * @param outputs the outputs that are present in the structure, previously converted
     */
    default T list(Map<String, IdentifierKey> keys, Map<String, OutputType> outputs) {
      return unknown();
    }

    /** The output is logs */
    default T logs(boolean optional) {
      return unknown();
    }

    /** The output is quality control information */
    default T qualityControl(boolean optional) {
      return unknown();
    }

    /**
     * The output is unknown
     *
     * <p>This will be used if implementations are not provided for the above methods and if future
     * methods are added.
     */
    T unknown();

    /** The output is data warehouse records */
    default T warehouseRecords(boolean optional) {
      return unknown();
    }
  }

  public static final class JacksonDeserializer extends JsonDeserializer<OutputType> {

    @Override
    public OutputType deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      return deserialize(jsonParser.readValueAsTree());
    }

    private OutputType deserialize(TreeNode node) {
      if (node.isValueNode() && ((ValueNode) node).isTextual()) {
        final var str = ((ValueNode) node).asText();
        switch (str) {
          case STR_FILE:
            return OutputType.FILE;
          case STR_FILES:
            return OutputType.FILES;
          case STR_FILE_WITH_LABELS:
            return OutputType.FILE_WITH_LABELS;
          case STR_FILES_WITH_LABELS:
            return OutputType.FILES_WITH_LABELS;
          case STR_LOGS:
            return OutputType.LOGS;
          case STR_QUALITY_CONTROL:
            return OutputType.QUALITY_CONTROL;
          case STR_UNKNOWN:
            return OutputType.UNKNOWN;
          case STR_WAREHOUSE_RECORDS:
            return OutputType.WAREHOUSE_RECORDS;
          case STR_FILE_OPTIONAL:
            return OutputType.FILE_OPTIONAL;
          case STR_FILES_OPTIONAL:
            return OutputType.FILES_OPTIONAL;
          case STR_FILE_WITH_LABELS_OPTIONAL:
            return OutputType.FILE_WITH_LABELS_OPTIONAL;
          case STR_FILES_WITH_LABELS_OPTIONAL:
            return OutputType.FILES_WITH_LABELS_OPTIONAL;
          case STR_LOGS_OPTIONAL:
            return OutputType.LOGS_OPTIONAL;
          case STR_QUALITY_CONTROL_OPTIONAL:
            return OutputType.QUALITY_CONTROL_OPTIONAL;
          case STR_WAREHOUSE_RECORDS_OPTIONAL:
            return OutputType.WAREHOUSE_RECORDS_OPTIONAL;
          default:
            throw new IllegalArgumentException("Unknown output type: " + str);
        }
      } else if (node.isObject() && node instanceof ObjectNode) {
        final var obj = (ObjectNode) node;
        if (obj.has(STR_IS) && obj.get(STR_IS).isTextual()) {
          switch (obj.get(STR_IS).asText()) {
            case STR_LIST:
              if (!obj.has(STR_KEYS)) {
                throw new IllegalArgumentException("List is missing 'keys' property");
              }
              if (!obj.has(STR_OUTPUTS)) {
                throw new IllegalArgumentException("List is missing 'outputs' property");
              }
              return list(
                  (StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(obj.get(STR_KEYS).fields(), 0), false)
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              e -> IdentifierKey.valueOf(e.getValue().asText().toUpperCase())))),
                  (StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(obj.get(STR_OUTPUTS).fields(), 0),
                          false)
                      .collect(
                          Collectors.toMap(Map.Entry::getKey, e -> deserialize(e.getValue())))));
            default:
              throw new IllegalArgumentException("Invalid 'is' in JSON object");
          }
        } else {
          throw new IllegalArgumentException("No 'is' in JSON object");
        }

      } else {
        throw new IllegalArgumentException("Invalid JSON token in output type: " + node);
      }
    }
  }

  public static final class JacksonSerializer extends JsonSerializer<OutputType> {
    private interface Printer {
      void print(JsonGenerator jsonGenerator) throws IOException;
    }

    @Override
    public void serialize(
        OutputType outputType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      outputType
          .apply(
              new Visitor<Printer>() {
                @Override
                public Printer file(boolean optional) {
                  return g -> g.writeString(optional ? STR_FILE_OPTIONAL : STR_FILE);
                }

                @Override
                public Printer fileWithLabels(boolean optional) {
                  return g ->
                      g.writeString(
                          optional ? STR_FILE_WITH_LABELS_OPTIONAL : STR_FILE_WITH_LABELS);
                }

                @Override
                public Printer files(boolean optional) {
                  return g -> g.writeString(optional ? STR_FILES_OPTIONAL : STR_FILES);
                }

                @Override
                public Printer filesWithLabels(boolean optional) {
                  return g ->
                      g.writeString(
                          optional ? STR_FILES_WITH_LABELS_OPTIONAL : STR_FILES_WITH_LABELS);
                }

                @Override
                public Printer list(
                    Map<String, IdentifierKey> keys, Map<String, OutputType> outputs) {
                  final var printOutputs =
                      outputs.entrySet().stream()
                          .collect(
                              Collectors.toMap(Map.Entry::getKey, e -> e.getValue().apply(this)));
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField(STR_IS, STR_LIST);
                    g.writeObjectFieldStart(STR_KEYS);
                    for (final var key : keys.entrySet()) {
                      g.writeStringField(key.getKey(), key.getValue().name().toLowerCase());
                    }
                    g.writeEndObject();
                    g.writeObjectFieldStart(STR_OUTPUTS);
                    for (final var output : printOutputs.entrySet()) {
                      g.writeFieldName(output.getKey());
                      output.getValue().print(g);
                    }
                    g.writeEndObject();
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer logs(boolean optional) {
                  return g -> g.writeString(optional ? STR_LOGS_OPTIONAL : STR_LOGS);
                }

                @Override
                public Printer qualityControl(boolean optional) {
                  return g ->
                      g.writeString(optional ? STR_QUALITY_CONTROL_OPTIONAL : STR_QUALITY_CONTROL);
                }

                @Override
                public Printer unknown() {
                  return g -> g.writeString(STR_UNKNOWN);
                }

                @Override
                public Printer warehouseRecords(boolean optional) {
                  return g ->
                      g.writeString(
                          optional ? STR_WAREHOUSE_RECORDS_OPTIONAL : STR_WAREHOUSE_RECORDS);
                }
              })
          .print(jsonGenerator);
    }
  }

  private static final class ListOutputType extends OutputType {
    final Map<String, IdentifierKey> inputKeys;
    final Map<String, OutputType> outputValues;

    private ListOutputType(
        Map<String, IdentifierKey> keys, Map<String, ? extends OutputType> outputs) {
      // TreeMap can't have null keys, so we only need to check values
      for (final var type : keys.values()) {
        Objects.requireNonNull(type, "list identifier key type");
      }
      for (final var type : outputs.values()) {
        Objects.requireNonNull(type, "list output type");
      }
      inputKeys = Collections.unmodifiableMap(new TreeMap<>(keys));
      outputValues = Collections.unmodifiableMap(new TreeMap<>(outputs));
    }

    @Override
    public <T> T apply(Visitor<T> visitor) {
      return visitor.list(inputKeys, outputValues);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ListOutputType that = (ListOutputType) o;
      return inputKeys.equals(that.inputKeys) && outputValues.equals(that.outputValues);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inputKeys, outputValues);
    }
  }
  /** The output is a single file */
  public static final OutputType FILE =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.file(false);
        }
      };
  /** The output is an optional single file */
  public static final OutputType FILE_OPTIONAL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.file(true);
        }
      };
  /** The output is a collection files that should have identical metadata */
  public static final OutputType FILES =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.files(false);
        }
      };
  /** The output is an optional collection files that should have identical metadata */
  public static final OutputType FILES_OPTIONAL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.files(true);
        }
      };
  /**
   * The output is a collection files with workflow-derived labels that should have identical
   * metadata
   */
  public static final OutputType FILES_WITH_LABELS =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.filesWithLabels(false);
        }
      };
  /**
   * The output is an optional collection files with workflow-derived labels that should have
   * identical metadata
   */
  public static final OutputType FILES_WITH_LABELS_OPTIONAL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.filesWithLabels(true);
        }
      };
  /** The output is a single files with workflow-derived labels */
  public static final OutputType FILE_WITH_LABELS =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.fileWithLabels(false);
        }
      };
  /** The output is an optional single files with workflow-derived labels */
  public static final OutputType FILE_WITH_LABELS_OPTIONAL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.fileWithLabels(true);
        }
      };
  /** The output is logs */
  public static final OutputType LOGS =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.logs(false);
        }
      };
  /** The output is optional logs */
  public static final OutputType LOGS_OPTIONAL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.logs(true);
        }
      };
  /** The output is quality control information */
  public static final OutputType QUALITY_CONTROL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.qualityControl(false);
        }
      };
  /** The output is optional quality control information */
  public static final OutputType QUALITY_CONTROL_OPTIONAL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.qualityControl(true);
        }
      };
  /** The output is unknown or an error */
  public static final OutputType UNKNOWN =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.unknown();
        }
      };
  /** The output is data warehouse records */
  public static final OutputType WAREHOUSE_RECORDS =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.warehouseRecords(false);
        }
      };
  /** The output is optional data warehouse records */
  public static final OutputType WAREHOUSE_RECORDS_OPTIONAL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.warehouseRecords(true);
        }
      };

  /**
   * The output is a list of structures
   *
   * @param keys the entries in the structure that are used to uniquely identify each one (a
   *     composite key)
   * @param outputs the entries in the structure
   */
  public static OutputType list(Map<String, IdentifierKey> keys, Map<String, OutputType> outputs) {
    // keys and outputs have to be non-empty
    if (null == keys || keys.isEmpty() || keys.containsKey(""))
      throw new IllegalArgumentException("ListOutputType cannot have empty keys");
    if (null == outputs || outputs.isEmpty() || outputs.containsKey(""))
      throw new IllegalArgumentException("ListOutputType cannot have empty outputs");
    if (keys.keySet().stream().anyMatch(outputs.keySet()::contains)) {
      throw new IllegalArgumentException("Overlap between input and output entry sets");
    }
    return new ListOutputType(keys, outputs);
  }

  private OutputType() {}

  /**
   * Transform the output type structure using the visitor
   *
   * @param visitor the visitor to apply the transformation
   * @param <T> the result type provided by the visitor
   * @return the transformed version of this type
   */
  public abstract <T> T apply(Visitor<T> visitor);
}
