package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class InputTypeTest {
  static Map<InputType, String> primitiveTypes =
      new HashMap<>() {
        {
          put(InputType.BOOLEAN, "boolean");
          put(InputType.DATE, "date");
          put(InputType.DIRECTORY, "directory");
          put(InputType.FILE, "file");
          put(InputType.FLOAT, "floating");
          put(InputType.INTEGER, "integer");
          put(InputType.JSON, "json");
          put(InputType.STRING, "string");
        }
      };
  static ObjectMapper MAPPER = new ObjectMapper();

  @BeforeClass
  public static void setUp() {
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  protected static void serializeTester(String expected, InputType toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.writeValueAsString(toTest));
    } catch (JsonProcessingException e) {
      Assert.fail("serializeTester threw JsonProcessingException: " + e.getMessage());
    }
  }

  protected static void deserializeTester(InputType expected, String toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.readValue(toTest, InputType.class));
    } catch (JsonProcessingException e) {
      Assert.fail("deserializeTester threw JsonProcessingException: " + e.getMessage());
    }
  }

  @Test
  public abstract void testSerialize();

  @Test
  public abstract void testDeserialize();

  @Test
  public abstract void testEquals();
}
