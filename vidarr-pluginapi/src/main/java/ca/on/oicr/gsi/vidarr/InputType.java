package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
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
import java.util.Comparator;
import java.util.Map;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Input type information that must be provided to run a workflow */
@JsonSerialize(using = InputType.JacksonSerializer.class)
@JsonDeserialize(using = InputType.JacksonDeserializer.class)
public abstract class InputType {
  /**
   * Convert an input type into another value
   *
   * @param <R> the return type to be generated
   */
  public interface Visitor<R> {

    /** Convert a <tt>boolean</tt> type */
    R bool();

    /** Convert a <tt>date</tt> type */
    R date();

    /**
     * Convert a map type
     *
     * @param key the type of the keys
     * @param value the type of the values
     */
    R dictionary(InputType key, InputType value);

    /** Convert a <tt>directory</tt> type */
    R directory();

    /** Convert a <tt>file</tt> type */
    R file();

    /** Convert a <tt>float</tt> type */
    R floating();

    /** Convert an <tt>integer</tt> type */
    R integer();

    /** Convert a <tt>json</tt> type */
    R json();

    /**
     * Convert a list type
     *
     * @param inner the type of the contents of the list
     */
    R list(InputType inner);

    /**
     * Convert an object type
     *
     * @param contents a list of fields in the object and their types
     */
    R object(Stream<Pair<String, InputType>> contents);

    /**
     * Convert an optional type
     *
     * @param inner the type inside the optional; may be null
     */
    R optional(InputType inner);

    /** Convert a pair of values */
    R pair(InputType left, InputType right);

    /** Convert a <tt>string</tt> type */
    R string();
    /**
     * Convert a discriminated union
     *
     * @param elements the possible values in the algebraic type
     */
    R taggedUnion(Stream<Map.Entry<String, InputType>> elements);

    /**
     * Convert a tuple type
     *
     * @param contents the types of the items in the tuple, in order
     */
    R tuple(Stream<InputType> contents);
  }

  private static final class DictionaryInputType extends InputType {
    private final InputType key;
    private final InputType value;

    DictionaryInputType(InputType key, InputType value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.dictionary(key, value);
    }
  }

  public static final class JacksonDeserializer extends JsonDeserializer<InputType> {

    @Override
    public InputType deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      return deserialize(jsonParser.readValueAsTree());
    }

    private InputType deserialize(TreeNode node) {
      if (node.isValueNode() && ((ValueNode) node).isTextual()) {
        final var str = ((ValueNode) node).asText();
        switch (str) {
          case "boolean":
            return InputType.BOOLEAN;
          case "date":
            return InputType.DATE;
          case "directory":
            return InputType.DIRECTORY;
          case "file":
            return InputType.FILE;
          case "floating":
            return InputType.FLOAT;
          case "integer":
            return InputType.INTEGER;
          case "json":
            return InputType.JSON;
          case "string":
            return InputType.STRING;
          default:
            throw new IllegalArgumentException("Unknown input type: " + str);
        }
      } else if (node.isObject() && node instanceof ObjectNode) {
        final var obj = (ObjectNode) node;
        if (obj.has("is") && obj.get("is").isTextual()) {
          switch (obj.get("is").asText()) {
            case "dictionary":
              return dictionary(deserialize(obj.get("key")), deserialize(obj.get("value")));
            case "list":
              return deserialize(obj.get("inner")).asList();
            case "object":
              return object(
                  StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(obj.get("fields").fields(), 0), false)
                      .map(e -> new Pair<>(e.getKey(), deserialize(e.getValue()))));
            case "optional":
              return deserialize(obj.get("inner")).asOptional();
            case "pair":
              return pair(deserialize(obj.get("left")), deserialize(obj.get("right")));
            case "tagged-union":
              return taggedUnionFromPairs(
                  StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(obj.get("fields").fields(), 0), false)
                      .map(e -> new Pair<>(e.getKey(), deserialize(e.getValue()))));
            case "tuple":
              return tuple(
                  StreamSupport.stream(obj.get("elements").spliterator(), false)
                      .map(this::deserialize)
                      .toArray(InputType[]::new));
            default:
              throw new IllegalArgumentException("Invalid 'is' in JSON object");
          }
        } else {
          throw new IllegalArgumentException("No 'is' in JSON object");
        }

      } else {
        throw new IllegalArgumentException("Invalid JSON token in input type: " + node);
      }
    }
  }

  public static final class JacksonSerializer extends JsonSerializer<InputType> {
    private interface Printer {
      void print(JsonGenerator jsonGenerator) throws IOException;
    }

    @Override
    public void serialize(
        InputType inputType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      inputType
          .apply(
              new Visitor<Printer>() {
                @Override
                public Printer bool() {
                  return g -> g.writeString("boolean");
                }

                @Override
                public Printer date() {
                  return g -> g.writeString("date");
                }

                @Override
                public Printer dictionary(InputType key, InputType value) {
                  final var printKey = key.apply(this);
                  final var printValue = value.apply(this);
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "dictionary");
                    g.writeFieldName("key");
                    printKey.print(g);
                    g.writeFieldName("value");
                    printValue.print(g);
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer directory() {
                  return g -> g.writeString("directory");
                }

                @Override
                public Printer file() {
                  return g -> g.writeString("file");
                }

                @Override
                public Printer floating() {
                  return g -> g.writeString("floating");
                }

                @Override
                public Printer integer() {
                  return g -> g.writeString("integer");
                }

                @Override
                public Printer json() {
                  return g -> g.writeString("json");
                }

                @Override
                public Printer list(InputType inner) {
                  final var printInner = inner.apply(this);
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "list");
                    g.writeFieldName("inner");
                    printInner.print(g);
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer object(Stream<Pair<String, InputType>> contents) {
                  final var fields =
                      contents
                          .map(p -> new Pair<>(p.first(), p.second().apply(this)))
                          .collect(Collectors.toList());
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "object");
                    g.writeObjectFieldStart("fields");
                    for (final var field : fields) {
                      g.writeFieldName(field.first());
                      field.second().print(g);
                    }
                    g.writeEndObject();
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer optional(InputType inner) {
                  final var printInner = inner.apply(this);
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "optional");
                    g.writeFieldName("inner");
                    printInner.print(g);
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer pair(InputType left, InputType right) {
                  final var printLeft = left.apply(this);
                  final var printRight = right.apply(this);
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "pair");
                    g.writeFieldName("left");
                    printLeft.print(g);
                    g.writeFieldName("right");
                    printRight.print(g);
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer string() {
                  return g -> g.writeString("string");
                }

                @Override
                public Printer taggedUnion(Stream<Map.Entry<String, InputType>> elements) {
                  final var unions =
                      elements.collect(
                          Collectors.toMap(Map.Entry::getKey, e -> e.getValue().apply(this)));
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "tagged-union");
                    g.writeObjectFieldStart("unions");
                    for (final var union : unions.entrySet()) {
                      g.writeFieldName(union.getKey());
                      union.getValue().print(g);
                    }
                    g.writeEndObject();
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer tuple(Stream<InputType> contents) {
                  final var elements =
                      contents.map(e -> e.apply(this)).collect(Collectors.toList());
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "tuple");
                    g.writeArrayFieldStart("elements");
                    for (final var element : elements) {
                      element.print(g);
                    }
                    g.writeEndArray();
                    g.writeEndObject();
                  };
                }
              })
          .print(jsonGenerator);
    }
  }

  private static final class ListInputType extends InputType {
    private final InputType inner;

    private ListInputType(InputType inner) {
      this.inner = inner;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.list(inner);
    }
  }

  private static final class ObjectInputType extends InputType {

    private final Map<String, Pair<InputType, Integer>> fields = new TreeMap<>();

    public ObjectInputType(Stream<Pair<String, InputType>> fields) {
      fields
          .sorted(Comparator.comparing(Pair::first))
          .forEach(
              new Consumer<>() {
                int index;

                @Override
                public void accept(Pair<String, InputType> pair) {
                  ObjectInputType.this.fields.put(pair.first(), new Pair<>(pair.second(), index++));
                }
              });
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.object(
          fields.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue().first())));
    }
  }

  private static final class OptionalInputType extends InputType {
    private final InputType inner;

    public OptionalInputType(InputType inner) {
      this.inner = inner;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.optional(inner);
    }

    @Override
    public InputType asOptional() {
      return this;
    }
  }

  private static final class PairInputType extends InputType {
    private final InputType left;
    private final InputType right;

    PairInputType(InputType left, InputType right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.pair(left, right);
    }
  }

  private static final class TupleInputType extends InputType {
    private final InputType[] types;

    private TupleInputType(InputType[] types) {
      this.types = types;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.tuple(Stream.of(types));
    }
  }

  /**
   * Create a new dictionary type
   *
   * @param key the type of the dictionary's keys
   * @param value the type of the dictionary's values
   */
  public static InputType dictionary(InputType key, InputType value) {
    return new DictionaryInputType(key, value);
  }

  /**
   * Create a new object type
   *
   * @param fields the names and types of the fields; duplicate names are not permitted
   */
  public static InputType object(Stream<Pair<String, InputType>> fields) {
    return new ObjectInputType(fields);
  }
  /**
   * Create a new object type
   *
   * @param fields the names and types of the fields; duplicate names are not permitted
   */
  @SafeVarargs
  public static InputType object(Pair<String, InputType>... fields) {
    return object(Stream.of(fields));
  }

  /**
   * A pair of values
   *
   * <p>This is not functionally different from a two-element tuple.
   *
   * @param left the type of the first/left value
   * @param right the type of the second/right value
   */
  public static InputType pair(InputType left, InputType right) {
    return new PairInputType(left, right);
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  public static InputType taggedUnion(Stream<Map.Entry<String, InputType>> elements) {
    return new InputType() {
      private final Map<String, InputType> union =
          Collections.unmodifiableMap(
              elements.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

      @Override
      public <R> R apply(Visitor<R> transformer) {
        return transformer.taggedUnion(union.entrySet().stream());
      }
    };
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  @SafeVarargs
  public static InputType taggedUnion(Map.Entry<String, InputType>... elements) {
    return taggedUnion(Stream.of(elements));
  }
  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  @SafeVarargs
  public static InputType taggedUnion(Pair<String, InputType>... elements) {
    return taggedUnionFromPairs(Stream.of(elements));
  }
  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  public static InputType taggedUnionFromPairs(Stream<Pair<String, InputType>> elements) {
    return new InputType() {
      private final Map<String, InputType> union =
          Collections.unmodifiableMap(
              elements.collect(Collectors.toMap(Pair::first, Pair::second)));

      @Override
      public <R> R apply(Visitor<R> transformer) {
        return transformer.taggedUnion(union.entrySet().stream());
      }
    };
  }

  /**
   * Create a tuple type from the types of its elements.
   *
   * @param types the element types, in order
   */
  public static InputType tuple(InputType... types) {
    return new TupleInputType(types);
  }

  /** The type of a Boolean value */
  public static final InputType BOOLEAN =
      new InputType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.bool();
        }
      };
  /** The type of a date, encoded as string containing an ISO-8601 date in UTC */
  public static final InputType DATE =
      new InputType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.date();
        }
      };
  /** The type of a directory to be made available as a unit */
  public static final InputType DIRECTORY =
      new InputType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.directory();
        }
      };
  /** The type of a single file */
  public static final InputType FILE =
      new InputType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.file();
        }
      };
  /**
   * The type of a floating-point number
   *
   * <p>The precision is not specified
   */
  public static final InputType FLOAT =
      new InputType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.floating();
        }
      };
  /**
   * The type of an integral number
   *
   * <p>Precision is not specified
   */
  public static final InputType INTEGER =
      new InputType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.integer();
        }
      };
  /** The type of arbitrary JSON content */
  public static final InputType JSON =
      new InputType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.json();
        }
      };
  /** The type of a string */
  public static final InputType STRING =
      new InputType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.string();
        }
      };

  private InputType() {}

  /**
   * Transform this type into a another representation
   *
   * @param transformer the converter for each type
   */
  public abstract <R> R apply(Visitor<R> transformer);

  /** Create a list type containing the current type. */
  public final InputType asList() {
    return new ListInputType(this);
  }
  /** Create an optional type containing the current type. */
  public InputType asOptional() {
    return new OptionalInputType(this);
  }
}
