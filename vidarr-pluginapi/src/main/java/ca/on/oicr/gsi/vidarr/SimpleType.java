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

/**
 * Primitive input information
 *
 * <p>This is used in three contents:
 *
 * <ul>
 *   <li>direct engine parameters
 *   <li>additional information required by output provisioners
 *   <li>additional information required by input provisioners for external files
 * </ul>
 */
@JsonSerialize(using = SimpleType.JacksonSerializer.class)
@JsonDeserialize(using = SimpleType.JacksonDeserializer.class)
public abstract class SimpleType {
  /**
   * Convert an engine type into another value
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
    R dictionary(SimpleType key, SimpleType value);

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
    R list(SimpleType inner);

    /**
     * Convert an object type
     *
     * @param contents a list of fields in the object and their types
     */
    R object(Stream<Pair<String, SimpleType>> contents);

    /**
     * Convert an optional type
     *
     * @param inner the type inside the optional; may be null
     */
    R optional(SimpleType inner);

    /** Convert a pair of values */
    R pair(SimpleType left, SimpleType right);

    /** Convert a <tt>string</tt> type */
    R string();
    /**
     * Convert a discriminated union
     *
     * @param elements the possible values in the algebraic type
     */
    R taggedUnion(Stream<Map.Entry<String, SimpleType>> elements);
    /**
     * Convert a tuple type
     *
     * @param contents the types of the items in the tuple, in order
     */
    R tuple(Stream<SimpleType> contents);
  }

  private static final class DictionarySimpleType extends SimpleType {
    private final SimpleType key;
    private final SimpleType value;

    DictionarySimpleType(SimpleType key, SimpleType value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.dictionary(key, value);
    }
  }

  public static final class JacksonDeserializer extends JsonDeserializer<SimpleType> {

    @Override
    public SimpleType deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      return deserialize(jsonParser.readValueAsTree());
    }

    private SimpleType deserialize(TreeNode node) {
      if (node.isValueNode() && ((ValueNode) node).isTextual()) {
        final var str = ((ValueNode) node).asText();
        switch (str) {
          case "boolean":
            return SimpleType.BOOLEAN;
          case "date":
            return SimpleType.DATE;
          case "floating":
            return SimpleType.FLOAT;
          case "integer":
            return SimpleType.INTEGER;
          case "json":
            return SimpleType.JSON;
          case "string":
            return SimpleType.STRING;
          default:
            throw new IllegalArgumentException("Unknown engine type: " + str);
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
                      .toArray(SimpleType[]::new));
            default:
              throw new IllegalArgumentException("Invalid 'is' in JSON object");
          }
        } else {
          throw new IllegalArgumentException("No 'is' in JSON object");
        }

      } else {
        throw new IllegalArgumentException("Invalid JSON token in engine type: " + node);
      }
    }
  }

  public static final class JacksonSerializer extends JsonSerializer<SimpleType> {
    private interface Printer {
      void print(JsonGenerator jsonGenerator) throws IOException;
    }

    @Override
    public void serialize(
        SimpleType simpleType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      simpleType
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
                public Printer dictionary(SimpleType key, SimpleType value) {
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
                public Printer list(SimpleType inner) {
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
                public Printer object(Stream<Pair<String, SimpleType>> contents) {
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
                public Printer optional(SimpleType inner) {
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
                public Printer pair(SimpleType left, SimpleType right) {
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
                public Printer taggedUnion(Stream<Map.Entry<String, SimpleType>> elements) {
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
                public Printer tuple(Stream<SimpleType> contents) {
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

  private static final class ListSimpleType extends SimpleType {
    private final SimpleType inner;

    private ListSimpleType(SimpleType inner) {
      this.inner = inner;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.list(inner);
    }
  }

  private static final class ObjectSimpleType extends SimpleType {

    private final Map<String, Pair<SimpleType, Integer>> fields = new TreeMap<>();

    public ObjectSimpleType(Stream<Pair<String, SimpleType>> fields) {
      fields
          .sorted(Comparator.comparing(Pair::first))
          .forEach(
              new Consumer<>() {
                int index;

                @Override
                public void accept(Pair<String, SimpleType> pair) {
                  ObjectSimpleType.this.fields.put(
                      pair.first(), new Pair<>(pair.second(), index++));
                }
              });
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.object(
          fields.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue().first())));
    }
  }

  private static final class OptionalSimpleType extends SimpleType {
    private final SimpleType inner;

    public OptionalSimpleType(SimpleType inner) {
      this.inner = inner;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.optional(inner);
    }

    @Override
    public SimpleType asOptional() {
      return this;
    }
  }

  private static final class PairSimpleType extends SimpleType {
    private final SimpleType left;
    private final SimpleType right;

    PairSimpleType(SimpleType left, SimpleType right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.pair(left, right);
    }
  }

  private static final class TupleSimpleType extends SimpleType {
    private final SimpleType[] types;

    private TupleSimpleType(SimpleType[] types) {
      this.types = types;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.tuple(Stream.of(types));
    }
  }

  /**
   * Create a dictionary type
   *
   * @param key the type of the keys
   * @param value the type of the values
   */
  public static SimpleType dictionary(SimpleType key, SimpleType value) {
    return new DictionarySimpleType(key, value);
  }

  /**
   * Create a new object type
   *
   * @param fields a collection of field names and the type for that field; duplicate field names
   *     are not permitted and will result in an exception
   */
  public static SimpleType object(Stream<Pair<String, SimpleType>> fields) {
    return new ObjectSimpleType(fields);
  }

  /**
   * Create a new object type
   *
   * @param fields a collection of field names and the type for that field; duplicate field names
   *     are not permitted and will result in an exception
   */
  @SafeVarargs
  public static SimpleType object(Pair<String, SimpleType>... fields) {
    return object(Stream.of(fields));
  }

  /**
   * Create a new pair type
   *
   * <p>This is functionally similar to a two-element tuple, but WDL has special encoding for pairs.
   *
   * @param left the type of the first/left element
   * @param right the type of teh second/right element
   */
  public static SimpleType pair(SimpleType left, SimpleType right) {
    return new PairSimpleType(left, right);
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  public static SimpleType taggedUnion(Stream<Map.Entry<String, SimpleType>> elements) {
    return new SimpleType() {
      private final Map<String, SimpleType> union =
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
  public static SimpleType taggedUnion(Map.Entry<String, SimpleType>... elements) {
    return taggedUnion(Stream.of(elements));
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  @SafeVarargs
  public static SimpleType taggedUnion(Pair<String, SimpleType>... elements) {
    return taggedUnionFromPairs(Stream.of(elements));
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  public static SimpleType taggedUnionFromPairs(Stream<Pair<String, SimpleType>> elements) {
    return new SimpleType() {
      private final Map<String, SimpleType> union =
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
  public static SimpleType tuple(SimpleType... types) {
    return new TupleSimpleType(types);
  }
  /** The type of a Boolean value */
  public static final SimpleType BOOLEAN =
      new SimpleType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.bool();
        }
      };
  /**
   * The type of a date
   *
   * <p>The expected encoding for a date is as an ISO-8601 timestamp in UTC
   */
  public static final SimpleType DATE =
      new SimpleType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.date();
        }
      };
  /**
   * The type of a floating-point number
   *
   * <p>Precision is not specified
   */
  public static final SimpleType FLOAT =
      new SimpleType() {

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
  public static final SimpleType INTEGER =
      new SimpleType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.integer();
        }
      };
  /** The type of arbitrary JSON content */
  public static final SimpleType JSON =
      new SimpleType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.json();
        }
      };
  /** The type of a string */
  public static final SimpleType STRING =
      new SimpleType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.string();
        }
      };

  private SimpleType() {}

  /**
   * Transform this type into a another representation
   *
   * @param transformer the converter for each type
   */
  public abstract <R> R apply(Visitor<R> transformer);

  /** Create a list type containing the current type. */
  public final SimpleType asList() {
    return new ListSimpleType(this);
  }

  /** Create an optional type containing the current type. */
  public SimpleType asOptional() {
    return new OptionalSimpleType(this);
  }
}
