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
 * Basic types that do not include files
 *
 * <p>This is used in three contents:
 *
 * <ul>
 *   <li>direct engine parameters
 *   <li>additional information required by output provisioners
 *   <li>additional information required by input provisioners for external files
 * </ul>
 */
@JsonSerialize(using = BasicType.JacksonSerializer.class)
@JsonDeserialize(using = BasicType.JacksonDeserializer.class)
public abstract class BasicType {
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
    R dictionary(BasicType key, BasicType value);

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
    R list(BasicType inner);

    /**
     * Convert an object type
     *
     * @param contents a list of fields in the object and their types
     */
    R object(Stream<Pair<String, BasicType>> contents);

    /**
     * Convert an optional type
     *
     * @param inner the type inside the optional; may be null
     */
    R optional(BasicType inner);

    /** Convert a pair of values */
    R pair(BasicType left, BasicType right);

    /** Convert a <tt>string</tt> type */
    R string();
    /**
     * Convert a discriminated union
     *
     * @param elements the possible values in the algebraic type
     */
    R taggedUnion(Stream<Map.Entry<String, BasicType>> elements);
    /**
     * Convert a tuple type
     *
     * @param contents the types of the items in the tuple, in order
     */
    R tuple(Stream<BasicType> contents);
  }

  private static final class DictionaryBasicType extends BasicType {
    private final BasicType key;
    private final BasicType value;

    DictionaryBasicType(BasicType key, BasicType value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.dictionary(key, value);
    }
  }

  public static final class JacksonDeserializer extends JsonDeserializer<BasicType> {

    @Override
    public BasicType deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      return deserialize(jsonParser.readValueAsTree());
    }

    private BasicType deserialize(TreeNode node) {
      if (node.isValueNode() && ((ValueNode) node).isTextual()) {
        final var str = ((ValueNode) node).asText();
        switch (str) {
          case "boolean":
            return BasicType.BOOLEAN;
          case "date":
            return BasicType.DATE;
          case "floating":
            return BasicType.FLOAT;
          case "integer":
            return BasicType.INTEGER;
          case "json":
            return BasicType.JSON;
          case "string":
            return BasicType.STRING;
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
                          Spliterators.spliteratorUnknownSize(obj.get("options").fields(), 0),
                          false)
                      .map(e -> new Pair<>(e.getKey(), deserialize(e.getValue()))));
            case "tuple":
              return tuple(
                  StreamSupport.stream(obj.get("elements").spliterator(), false)
                      .map(this::deserialize)
                      .toArray(BasicType[]::new));
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

  public static final class JacksonSerializer extends JsonSerializer<BasicType> {
    private interface Printer {
      void print(JsonGenerator jsonGenerator) throws IOException;
    }

    @Override
    public void serialize(
        BasicType basicType, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      basicType
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
                public Printer dictionary(BasicType key, BasicType value) {
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
                public Printer list(BasicType inner) {
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
                public Printer object(Stream<Pair<String, BasicType>> contents) {
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
                public Printer optional(BasicType inner) {
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
                public Printer pair(BasicType left, BasicType right) {
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
                public Printer taggedUnion(Stream<Map.Entry<String, BasicType>> elements) {
                  final var unions =
                      elements.collect(
                          Collectors.toMap(Map.Entry::getKey, e -> e.getValue().apply(this)));
                  return g -> {
                    g.writeStartObject();
                    g.writeStringField("is", "tagged-union");
                    g.writeObjectFieldStart("options");
                    for (final var union : unions.entrySet()) {
                      g.writeFieldName(union.getKey());
                      union.getValue().print(g);
                    }
                    g.writeEndObject();
                    g.writeEndObject();
                  };
                }

                @Override
                public Printer tuple(Stream<BasicType> contents) {
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

  private static final class ListBasicType extends BasicType {
    private final BasicType inner;

    private ListBasicType(BasicType inner) {
      this.inner = inner;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.list(inner);
    }
  }

  private static final class ObjectBasicType extends BasicType {

    private final Map<String, Pair<BasicType, Integer>> fields = new TreeMap<>();

    public ObjectBasicType(Stream<Pair<String, BasicType>> fields) {
      fields
          .sorted(Comparator.comparing(Pair::first))
          .forEach(
              new Consumer<>() {
                int index;

                @Override
                public void accept(Pair<String, BasicType> pair) {
                  ObjectBasicType.this.fields.put(pair.first(), new Pair<>(pair.second(), index++));
                }
              });
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.object(
          fields.entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue().first())));
    }
  }

  private static final class OptionalBasicType extends BasicType {
    private final BasicType inner;

    public OptionalBasicType(BasicType inner) {
      this.inner = inner;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.optional(inner);
    }

    @Override
    public BasicType asOptional() {
      return this;
    }
  }

  private static final class PairBasicType extends BasicType {
    private final BasicType left;
    private final BasicType right;

    PairBasicType(BasicType left, BasicType right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public <R> R apply(Visitor<R> transformer) {
      return transformer.pair(left, right);
    }
  }

  private static final class TupleBasicType extends BasicType {
    private final BasicType[] types;

    private TupleBasicType(BasicType[] types) {
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
  public static BasicType dictionary(BasicType key, BasicType value) {
    return new DictionaryBasicType(key, value);
  }

  /**
   * Create a new object type
   *
   * @param fields a collection of field names and the type for that field; duplicate field names
   *     are not permitted and will result in an exception
   */
  public static BasicType object(Stream<Pair<String, BasicType>> fields) {
    return new ObjectBasicType(fields);
  }

  /**
   * Create a new object type
   *
   * @param fields a collection of field names and the type for that field; duplicate field names
   *     are not permitted and will result in an exception
   */
  @SafeVarargs
  public static BasicType object(Pair<String, BasicType>... fields) {
    return object(Stream.of(fields));
  }

  /**
   * Create a new pair type
   *
   * <p>This is functionally similar to a two-element tuple, but WDL has special encoding for pairs.
   *
   * @param left the type of the first/left element
   * @param right the type of the second/right element
   */
  public static BasicType pair(BasicType left, BasicType right) {
    return new PairBasicType(left, right);
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  public static BasicType taggedUnion(Stream<Map.Entry<String, BasicType>> elements) {
    return new BasicType() {
      private final Map<String, BasicType> union =
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
  public static BasicType taggedUnion(Map.Entry<String, BasicType>... elements) {
    return taggedUnion(Stream.of(elements));
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  @SafeVarargs
  public static BasicType taggedUnion(Pair<String, BasicType>... elements) {
    return taggedUnionFromPairs(Stream.of(elements));
  }

  /**
   * The output is a choice between multiple tagged data structures
   *
   * @param elements the possible data structures; the string identifiers must be unique
   */
  public static BasicType taggedUnionFromPairs(Stream<Pair<String, BasicType>> elements) {
    return new BasicType() {
      private final Map<String, BasicType> union =
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
  public static BasicType tuple(BasicType... types) {
    return new TupleBasicType(types);
  }
  /** The type of a Boolean value */
  public static final BasicType BOOLEAN =
      new BasicType() {

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
  public static final BasicType DATE =
      new BasicType() {

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
  public static final BasicType FLOAT =
      new BasicType() {

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
  public static final BasicType INTEGER =
      new BasicType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.integer();
        }
      };
  /** The type of arbitrary JSON content */
  public static final BasicType JSON =
      new BasicType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.json();
        }
      };
  /** The type of a string */
  public static final BasicType STRING =
      new BasicType() {

        @Override
        public <R> R apply(Visitor<R> transformer) {
          return transformer.string();
        }
      };

  private BasicType() {}

  /**
   * Transform this type into a another representation
   *
   * @param transformer the converter for each type
   */
  public abstract <R> R apply(Visitor<R> transformer);

  /** Create a list type containing the current type. */
  public final BasicType asList() {
    return new ListBasicType(this);
  }

  /** Create an optional type containing the current type. */
  public BasicType asOptional() {
    return new OptionalBasicType(this);
  }
}
