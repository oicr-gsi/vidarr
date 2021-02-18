package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An unload filter a JSON value that comes in two forms:
 *
 * <ul>
 *   <li>a string for a direct match
 *   <li>an array of strings for a "any in set" match
 * </ul>
 */
@JsonDeserialize(using = UnloadTextSelector.JacksonDeserializer.class)
@JsonSerialize(using = UnloadTextSelector.JacksonSerializer.class)
public abstract class UnloadTextSelector {
  public static final class JacksonDeserializer extends JsonDeserializer<UnloadTextSelector> {

    @Override
    public UnloadTextSelector deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      final var tree = jsonParser.readValueAsTree();
      if (tree.isValueNode() && ((ValueNode) tree).isTextual()) {
        return of(((ValueNode) tree).asText());
      }
      if (tree.isArray()) {
        final var values = new TreeSet<String>();
        for (final var child : ((ArrayNode) tree)) {
          if (child.isValueNode() && child.isTextual()) {
            values.add(child.asText());
          } else {
            throw new IllegalArgumentException("Non-string element in array unload filter array.");
          }
        }
        return of(values);
      }
      throw new IllegalArgumentException("JSON value is not valid unload filter.");
    }
  }

  public static final class JacksonSerializer extends JsonSerializer<UnloadTextSelector> {

    @Override
    public void serialize(
        UnloadTextSelector filter,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider)
        throws IOException {
      filter.toJson(jsonGenerator);
    }
  }

  /**
   * Create a new unload filter for a fixed string
   *
   * @param value the string to match
   */
  public static UnloadTextSelector of(String value) {
    return new UnloadTextSelector() {

      @Override
      public Stream<String> stream() {
        return Stream.of(value);
      }

      @Override
      protected void toJson(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeString(value);
      }
    };
  }

  /**
   * Create a new unload filter from a collection of items
   *
   * @param values the items to match
   */
  public static UnloadTextSelector of(Collection<String> values) {
    return of(values.stream());
  }

  /**
   * Create a new unload filter from a collection of items
   *
   * @param values the items to match
   */
  public static UnloadTextSelector of(Stream<String> values) {
    return new UnloadTextSelector() {
      private final Set<String> items = values.collect(Collectors.toCollection(TreeSet::new));

      @Override
      public Stream<String> stream() {
        return items.stream();
      }

      @Override
      protected void toJson(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeStartArray();
        for (final var item : items) {
          jsonGenerator.writeString(item);
        }
        jsonGenerator.writeEndArray();
      }
    };
  }

  private UnloadTextSelector() {}

  /** Get all values in this filter */
  public abstract Stream<String> stream();

  protected abstract void toJson(JsonGenerator jsonGenerator) throws IOException;
}
