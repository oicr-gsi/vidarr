package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

public abstract class BasicTypeTest {
  static Map<BasicType, String> primitiveTypes =
      Map.of(
          BasicType.BOOLEAN, "boolean",
          BasicType.DATE, "date",
          BasicType.FLOAT, "floating",
          BasicType.INTEGER, "integer",
          BasicType.JSON, "json",
          BasicType.STRING, "string");
  static final JsonMapper MAPPER =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .build();

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
