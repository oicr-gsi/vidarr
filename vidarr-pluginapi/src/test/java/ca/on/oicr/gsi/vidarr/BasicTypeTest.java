package ca.on.oicr.gsi.vidarr;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class BasicTypeTest {
  static Map<BasicType, String> primitiveTypes =
      Map.of(
          BasicType.BOOLEAN, "boolean",
          BasicType.DATE, "date",
          BasicType.FLOAT, "floating",
          BasicType.INTEGER, "integer",
          BasicType.JSON, "json",
          BasicType.STRING, "string");
  static ObjectMapper MAPPER = new ObjectMapper();

  @BeforeClass
  public static void setUp() {
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  protected static void serializeTester(String expected, BasicType toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.writeValueAsString(toTest));
    } catch (JacksonException e) {
      Assert.fail("serializeTester threw JacksonException: " + e.getMessage());
    }
  }

  protected static void deserializeTester(BasicType expected, String toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.readValue(toTest, BasicType.class));
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
