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
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Input type information that must be provided to run a workflow */
@JsonSerialize(using = InputType.JacksonSerializer.class)
@JsonDeserialize(using = InputType.JacksonDeserializer.class)
public abstract class InputType {

  private static final String STR_BOOLEAN = "boolean",
      STR_DATE = "date",
      STR_DIRECTORY = "directory",
      STR_FILE = "file",
      STR_FLOATING = "floating",
      STR_INTEGER = "integer",
      STR_JSON = "json",
      STR_STRING = "string",
      STR_DICTIONARY = "dictionary",
      STR_OBJECT = "object",
      STR_PAIR = "pair",
      STR_RETRY = "retry",
      STR_TAGGED_UNION = "tagged-union",
      STR_TUPLE = "tuple",
      STR_LIST = "list",
      STR_OPTIONAL = "optional",
      STR_IS = "is",
      STR_KEY = "key",
      STR_VALUE = "value",
      STR_INNER = "inner",
      STR_FIELDS = "fields",
      STR_LEFT = "left",
      STR_RIGHT = "right",
      STR_OPTIONS = "options",
      STR_ELEMENTS = "elements";
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

    /**
     * Convert a type that can have multiple values for retrying the same value
     *
     * @param inner the type that can be retried
     */
    R retry(BasicType inner);

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
      Objects.requireNonNull(key, "key type");
      Objects.requireNonNull(value, "value type");
      this.key = key;
      this.value = value;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.dictionary(key, value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DictionaryInputType that = (DictionaryInputType) o;
      return key.equals(that.key) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
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
          case STR_BOOLEAN:
            return InputType.BOOLEAN;
          case STR_DATE:
            return InputType.DATE;
          case STR_DIRECTORY:
            return InputType.DIRECTORY;
          case STR_FILE:
            return InputType.FILE;
          case STR_FLOATING:
            return InputType.FLOAT;
          case STR_INTEGER:
            return InputType.INTEGER;
          case STR_JSON:
            return InputType.JSON;
          case STR_STRING:
            return InputType.STRING;
          default:
            throw new IllegalArgumentException("Unknown input type: " + str);
        }
      } else if (node.isObject() && node instanceof ObjectNode) {
        final var obj = (ObjectNode) node;
        if (obj.has(STR_IS) && obj.get(STR_IS).isTextual()) {
          switch (obj.get(STR_IS).asText()) {
            case STR_DICTIONARY:
              if (!obj.has(STR_KEY)) {
                throw new IllegalArgumentException("Missing 'key' in dictionary.");
              }
              if (!obj.has(STR_VALUE)) {
                throw new IllegalArgumentException("Missing 'value' in dictionary.");
              }
              return dictionary(deserialize(obj.get(STR_KEY)), deserialize(obj.get(STR_VALUE)));
            case STR_LIST:
              if (!obj.has(STR_INNER)) {
                throw new IllegalArgumentException("Missing 'inner' in list.");
              }
              return deserialize(obj.get(STR_INNER)).asList();
            case STR_OBJECT:
              if (!obj.has(STR_FIELDS)) {
                throw new IllegalArgumentException("Missing 'fields' in object.");
              }
              return object(
                  StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(obj.get(STR_FIELDS).fields(), 0),
                          false)
                      .map(e -> new Pair<>(e.getKey(), deserialize(e.getValue()))));
            case STR_OPTIONAL:
              if (!obj.has(STR_INNER)) {
                throw new IllegalArgumentException("Missing 'inner' in optional.");
              }
              return deserialize(obj.get(STR_INNER)).asOptional();
            case STR_PAIR:
              if (!obj.has(STR_LEFT)) {
                throw new IllegalArgumentException("Missing 'left' in pair.");
              }
              if (!obj.has(STR_RIGHT)) {
                throw new IllegalArgumentException("Missing 'right' in pair.");
              }
              return pair(deserialize(obj.get(STR_LEFT)), deserialize(obj.get(STR_RIGHT)));
            case STR_RETRY:
              if (!obj.has(STR_INNER)) {
                throw new IllegalArgumentException("Missing 'inner' in retry.");
              }
              return retry(BasicType.deserialize(obj.get(STR_INNER)));
            case STR_TAGGED_UNION:
              if (!obj.has(STR_OPTIONS)) {
                throw new IllegalArgumentException("Missing 'options' in tagged union.");
              }
              return taggedUnionFromPairs(
                  StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(obj.get(STR_OPTIONS).fields(), 0),
                          false)
                      .map(e -> new Pair<>(e.getKey(), deserialize(e.getValue()))));
            case STR_TUPLE:
              if (!obj.has(STR_ELEMENTS)) {
                throw new IllegalArgumentException("Missing 'elements' in tuple.");
              }
              return tuple(
                  StreamSupport.stream(obj.get(STR_ELEMENTS).spliterator(), false)
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

    @Override
    public void serialize(
        InputType inputType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      inputType
          .apply(
              new Visitor<Printer>() {
                @Override
                public Printer bool() {
                  return g -> g.writeString(STR_BOOLEAN);
                }

                @Override
                public Printer date() {
                  return g -> g.writeString(STR_DATE);
                }

                @Override
                public Printer dictionary(InputType key, InputType value) {
                  final var printKey = key.apply(this);
                  final var printValue = value.apply(this);
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField(STR_IS, STR_DICTIONARY);
                    g.writeFieldName(STR_KEY);
                    printKey.print(g);
                    g.writeFieldName(STR_VALUE);
                    printValue.print(g);
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer directory() {
                  return g -> g.writeString(STR_DIRECTORY);
                }

                @Override
                public Printer file() {
                  return g -> g.writeString(STR_FILE);
                }

                @Override
                public Printer floating() {
                  return g -> g.writeString(STR_FLOATING);
                }

                @Override
                public Printer integer() {
                  return g -> g.writeString(STR_INTEGER);
                }

                @Override
                public Printer json() {
                  return g -> g.writeString(STR_JSON);
                }

                @Override
                public Printer list(InputType inner) {
                  final var printInner = inner.apply(this);
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField(STR_IS, STR_LIST);
                    g.writeFieldName(STR_INNER);
                    printInner.print(g);
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer object(Stream<Pair<String, InputType>> contents) {
                  final var fields =
                      contents
                          .map(p -> new Pair<>(p.first(), p.second().apply(this)))
                          .toList();
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField(STR_IS, STR_OBJECT);
                    g.writeObjectFieldStart(STR_FIELDS);
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
                    g.writeStringField(STR_IS, STR_OPTIONAL);
                    g.writeFieldName(STR_INNER);
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
                    g.writeStringField(STR_IS, STR_PAIR);
                    g.writeFieldName(STR_LEFT);
                    printLeft.print(g);
                    g.writeFieldName(STR_RIGHT);
                    printRight.print(g);
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer retry(BasicType inner) {
                  final var printInner = inner.apply(BasicType.CREATE_PRINTER);
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField(STR_IS, STR_RETRY);
                    g.writeFieldName(STR_INNER);
                    printInner.print(g);
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer string() {
                  return g -> g.writeString(STR_STRING);
                }

                @Override
                public Printer taggedUnion(Stream<Map.Entry<String, InputType>> elements) {
                  final var unions =
                      elements.collect(
                          Collectors.toMap(Map.Entry::getKey, e -> e.getValue().apply(this)));
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField(STR_IS, STR_TAGGED_UNION);
                    g.writeObjectFieldStart(STR_OPTIONS);
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
                      contents.map(e -> e.apply(this)).toList();
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField(STR_IS, STR_TUPLE);
                    g.writeArrayFieldStart(STR_ELEMENTS);
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ListInputType that = (ListInputType) o;
      return inner.equals(that.inner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inner);
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
                  Objects.requireNonNull(pair.first(), "object field name");
                  Objects.requireNonNull(pair.second(), "object field type");
                  ObjectInputType.this.fields.put(pair.first(), new Pair<>(pair.second(), index++));
                }
              });
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.object(
          fields.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue().first())));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ObjectInputType that = (ObjectInputType) o;
      return fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fields);
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

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OptionalInputType that = (OptionalInputType) o;
      return inner.equals(that.inner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inner);
    }
  }

  private static final class PairInputType extends InputType {
    private final InputType left;
    private final InputType right;

    PairInputType(InputType left, InputType right) {
      Objects.requireNonNull(left, "pair left");
      Objects.requireNonNull(right, "pair right");
      this.left = left;
      this.right = right;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.pair(left, right);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PairInputType that = (PairInputType) o;
      return left.equals(that.left) && right.equals(that.right);
    }

    @Override
    public int hashCode() {
      return Objects.hash(left, right);
    }
  }

  private static final class RetryInputType extends InputType {
    private final BasicType inner;

    public RetryInputType(BasicType inner) {
      this.inner = inner;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.retry(inner);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RetryInputType that = (RetryInputType) o;
      return inner.equals(that.inner);
    }

    @Override
    public int hashCode() {
      return 33 * Objects.hash(inner);
    }
  }

  private static final class TaggedUnionInputType extends InputType {
    private final Map<String, InputType> union;

    private TaggedUnionInputType(TreeMap<String, InputType> union) {
      // TreeMap can't have null keys, so we only need to check values
      for (final var type : union.values()) {
        Objects.requireNonNull(type, "union type contents");
      }

      if (union.size() == 0)
        throw new IllegalArgumentException("TaggedUnion InputType needs at least 1 field, got 0.");

      if (union.containsKey(""))
        throw new IllegalArgumentException(
            "Found illegal field key \"\" while creating TaggedUnion InputType.");

      this.union = Collections.unmodifiableMap(union);
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.taggedUnion(union.entrySet().stream());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TaggedUnionInputType that = (TaggedUnionInputType) o;
      return union.equals(that.union);
    }

    @Override
    public int hashCode() {
      return Objects.hash(union);
    }
  }

  private static final class TupleInputType extends InputType {
    private final InputType[] types;

    private TupleInputType(InputType[] types) {
      this.types = types;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TupleInputType that = (TupleInputType) o;
      return Arrays.equals(this.types, that.types);
    }

    @Override
    public int hashCode() {
      return Objects.hash(types);
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.tuple(Stream.of(types));
    }
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
   * @throws IllegalArgumentException if given a Stream with 0 elements, or a field key is empty
   *     string
   */
  public static InputType object(Stream<Pair<String, InputType>> fields) {
    // Sanity checking
    final List<Pair<String, InputType>> fieldsList = fields.toList();
    if (fieldsList.isEmpty())
      throw new IllegalArgumentException("Object InputType needs at least 1 field, got 0.");

    for (final Map.Entry<String, Long> entry :
        fieldsList.stream()
            .collect(Collectors.groupingBy(Pair::first, Collectors.counting()))
            .entrySet()) {
      if (entry.getKey().isEmpty()) {
        throw new IllegalArgumentException(
            "Found illegal field key \"\" while creating Object InputType.");
      }
      if (entry.getValue() > 1) {
        throw new IllegalArgumentException(
            "Found illegal duplicate key(s) "
                + entry.getValue()
                + " while creating Object InputType.");
      }
    }

    return new ObjectInputType(fieldsList.stream());
  }

  /**
   * Create a new object type
   *
   * @param fields the names and types of the fields; duplicate names are not permitted
   * @throws IllegalArgumentException if given a Stream with 0 elements
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

  public static InputType retry(BasicType inner) {
    Objects.requireNonNull(inner, "retry type contents");
    return new RetryInputType(inner);
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  public static InputType taggedUnion(Stream<Map.Entry<String, InputType>> elements) {
    return new TaggedUnionInputType(
        elements.collect(
            Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (a, b) -> {
                  throw new IllegalArgumentException("Duplicate identifier in tagged union.");
                },
                TreeMap::new)));
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
    return new TaggedUnionInputType(
        elements.collect(
            Collectors.toMap(
                Pair::first,
                Pair::second,
                (a, b) -> {
                  throw new IllegalArgumentException("Duplicate identifier in tagged union.");
                },
                TreeMap::new)));
  }

  /**
   * Create a tuple type from the types of its elements.
   *
   * @param types the element types, in order
   */
  public static InputType tuple(InputType... types) {
    Objects.requireNonNull(types, "tuple types");
    for (final var type : types) {
      Objects.requireNonNull(type, "tuple types");
    }
    return new TupleInputType(types.clone());
  }

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
