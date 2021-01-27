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
    default T file() {
      return unknown();
    }

    /** The output is a pair of a single files and a dictionary of labels */
    default T fileWithLabels() {
      return unknown();
    }

    /** The output is a list of files */
    default T files() {
      return unknown();
    }

    /** The output is a pair of a list of files and a dictionary of labels */
    default T filesWithLabels() {
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
    default T logs() {
      return unknown();
    }

    /** The output is quality control information */
    default T qualityControl() {
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
    default T warehouseRecords() {
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
          case "file":
            return OutputType.FILE;
          case "files":
            return OutputType.FILES;
          case "file-with-labels":
            return OutputType.FILE_WITH_LABELS;
          case "files-with-labels":
            return OutputType.FILES_WITH_LABELS;
          case "logs":
            return OutputType.LOGS;
          case "quality-control":
            return OutputType.QUALITY_CONTROL;
          case "unknown":
            return OutputType.UNKNOWN;
          case "warehouse-records":
            return OutputType.WAREHOUSE_RECORDS;
          default:
            throw new IllegalArgumentException("Unknown output type: " + str);
        }
      } else if (node.isObject() && node instanceof ObjectNode) {
        final var obj = (ObjectNode) node;
        if (obj.has("is") && obj.get("is").isTextual()) {
          switch (obj.get("is").asText()) {
            case "list":
              return list(
                  (StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(obj.get("keys").fields(), 0), false)
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey,
                              e -> IdentifierKey.valueOf(e.getValue().asText().toUpperCase())))),
                  (StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(obj.get("outputs").fields(), 0),
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
        OutputType outputType,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException {
      outputType
          .apply(
              new Visitor<Printer>() {
                @Override
                public Printer file() {
                  return g -> g.writeString("file");
                }

                @Override
                public Printer fileWithLabels() {
                  return g -> g.writeString("file-with-labels");
                }

                @Override
                public Printer files() {
                  return g -> g.writeString("files");
                }

                @Override
                public Printer filesWithLabels() {
                  return g -> g.writeString("files-with-labels");
                }

                @Override
                public Printer list(
                    Map<String, IdentifierKey> keys, Map<String, OutputType> outputs) {
                  final var printOuputs =
                      outputs.entrySet().stream()
                          .collect(
                              Collectors.toMap(Map.Entry::getKey, e -> e.getValue().apply(this)));
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "list");
                    g.writeObjectFieldStart("keys");
                    for (final var key : keys.entrySet()) {
                      g.writeStringField(key.getKey(), key.getValue().name().toLowerCase());
                    }
                    g.writeEndObject();
                    g.writeObjectFieldStart("outputs");
                    for (final var output : printOuputs.entrySet()) {
                      g.writeFieldName(output.getKey());
                      output.getValue().print(g);
                    }
                    g.writeEndObject();
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer logs() {
                  return g -> g.writeString("logs");
                }

                @Override
                public Printer qualityControl() {
                  return g -> g.writeString("quality-control");
                }

                @Override
                public Printer unknown() {
                  return g -> g.writeString("unknown");
                }

                @Override
                public Printer warehouseRecords() {
                  return g -> g.writeString("warehouse-records");
                }
              })
          .print(jsonGenerator);
    }
  }

  /**
   * The output is a list of structures
   *
   * @param keys the entries in the structure that are used to uniquely identify each one (a
   *     composite key)
   * @param outputs the entries in the structure
   */
  public static OutputType list(
      Map<String, IdentifierKey> keys, Map<String, OutputType> outputs) {
    if (keys.keySet().stream().anyMatch(outputs.keySet()::contains)) {
      throw new IllegalArgumentException("Overlap between input and output entry sets");
    }
    return new OutputType() {
      final Map<String, IdentifierKey> inputKeys = Collections.unmodifiableMap(new TreeMap<>(keys));
      final Map<String, OutputType> outputValues =
          Collections.unmodifiableMap(new TreeMap<>(outputs));

      @Override
      public <T> T apply(Visitor<T> visitor) {
        return visitor.list(inputKeys, outputValues);
      }
    };
  }

  /** The output is a single file */
  public static final OutputType FILE =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.file();
        }
      };

  /** The output is a collection files that should have identical metadata */
  public static final OutputType FILES =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.files();
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
          return visitor.filesWithLabels();
        }
      };
  /** The output is a single files with workflow-derived labels */
  public static final OutputType FILE_WITH_LABELS =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.fileWithLabels();
        }
      };
  /** The output is logs */
  public static final OutputType LOGS =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.logs();
        }
      };

  /** The output is quality control information */
  public static final OutputType QUALITY_CONTROL =
      new OutputType() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.qualityControl();
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
          return visitor.warehouseRecords();
        }
      };

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
