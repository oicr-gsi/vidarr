package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.core.JsonPath.JsonPathDeserializer;
import ca.on.oicr.gsi.vidarr.core.JsonPath.JsonPathSerializer;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

/** A description of how to select a child element in a larger JSON structure */
@JsonDeserialize(using = JsonPathDeserializer.class)
@JsonSerialize(using = JsonPathSerializer.class)
public abstract class JsonPath {
  public static final class JsonPathDeserializer extends JsonDeserializer<JsonPath> {

    @Override
    public JsonPath deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      return switch (jsonParser.currentToken()) {
        case VALUE_NUMBER_INT -> array(jsonParser.getIntValue());
        case VALUE_STRING -> object(jsonParser.getText());
        default -> throw new IllegalArgumentException("Invalid path operation");
      };
    }
  }

  public static final class JsonPathSerializer extends JsonSerializer<JsonPath> {

    @Override
    public void serialize(
        JsonPath jsonPath, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      jsonPath.serialise(jsonGenerator);
    }
  }

  /**
   * The JSON element is an array; select an element
   *
   * @param index the index of the element to select
   */
  static JsonPath array(int index) {
    return new JsonPath() {
      @Override
      public JsonNode get(JsonNode parent) {
        return parent.get(index);
      }

      @Override
      public void serialise(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeNumber(index);
      }

      @Override
      public void set(JsonNode parent, JsonNode value) {
        ((ArrayNode) parent).set(index, value);
      }
    };
  }

  /**
   * The JSON element is an object; select a property
   *
   * @param field the name of the property
   */
  static JsonPath object(String field) {
    return new JsonPath() {
      @Override
      public JsonNode get(JsonNode parent) {
        return parent.get(field);
      }

      @Override
      public void serialise(JsonGenerator jsonGenerator) throws IOException {
        jsonGenerator.writeString(field);
      }

      @Override
      public void set(JsonNode parent, JsonNode value) {
        ((ObjectNode) parent).set(field, value);
      }
    };
  }

  private JsonPath() {}

  /**
   * Find the appropriate child node given the parent
   *
   * @param parent the parent node to apply the path on
   * @return the child node
   */
  public abstract JsonNode get(JsonNode parent);

  /**
   * Write this operation to a JSON
   *
   * @param jsonGenerator the generator to write to
   */
  public abstract void serialise(JsonGenerator jsonGenerator) throws IOException;

  /**
   * Mutate an element in a JSON structure
   *
   * @param parent the parent node to mutate
   * @param value the value to set the path to
   */
  public abstract void set(JsonNode parent, JsonNode value);
}
