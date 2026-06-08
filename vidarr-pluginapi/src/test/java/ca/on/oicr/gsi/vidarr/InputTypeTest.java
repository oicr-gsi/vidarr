package ca.on.oicr.gsi.vidarr;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class InputTypeTest {
  static Map<InputType, String> primitiveTypes =
      Map.of(
          InputType.BOOLEAN, "boolean",
          InputType.DATE, "date",
          InputType.DIRECTORY, "directory",
          InputType.FILE, "file",
          InputType.FLOAT, "floating",
          InputType.INTEGER, "integer",
          InputType.JSON, "json",
          InputType.STRING, "string");
  static ObjectMapper MAPPER = new ObjectMapper();

  @BeforeClass
  public static void setUp() {
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  protected static void serializeTester(String expected, InputType toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.writeValueAsString(toTest));
    } catch (JacksonException e) {
      Assert.fail("serializeTester threw JacksonException: " + e.getMessage());
    }
  }

  protected static void deserializeTester(InputType expected, String toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.readValue(toTest, InputType.class));
    } catch (JacksonException e) {
      Assert.fail("deserializeTester threw JacksonException: " + e.getMessage());
    }
  }

  @Test
  public abstract void testSerialize();

  @Test
  public abstract void testDeserialize();

  @Test
  public abstract void testEquals();
}
