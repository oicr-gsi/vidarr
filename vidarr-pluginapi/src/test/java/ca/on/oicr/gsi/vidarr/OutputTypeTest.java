package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.OutputType.IdentifierKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.AbstractMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class OutputTypeTest {
  static Map<IdentifierKey, String> identifierKeys =
      Map.of(
          IdentifierKey.INTEGER, "integer",
          IdentifierKey.STRING, "string");
  // Map.of() is only overloaded up to 10 pairs.
  static Map<OutputType, String> primitiveTypes =
      Map.ofEntries(
          new AbstractMap.SimpleEntry<>(OutputType.FILE, "file"),
          new AbstractMap.SimpleEntry<>(OutputType.FILES, "files"),
          new AbstractMap.SimpleEntry<>(OutputType.FILE_WITH_LABELS, "file-with-labels"),
          new AbstractMap.SimpleEntry<>(OutputType.FILES_WITH_LABELS, "files-with-labels"),
          new AbstractMap.SimpleEntry<>(OutputType.LOGS, "logs"),
          new AbstractMap.SimpleEntry<>(OutputType.QUALITY_CONTROL, "quality-control"),
          new AbstractMap.SimpleEntry<>(OutputType.UNKNOWN, "unknown"),
          new AbstractMap.SimpleEntry<>(OutputType.WAREHOUSE_RECORDS, "warehouse-records"),
          new AbstractMap.SimpleEntry<>(OutputType.FILE_OPTIONAL, "optional-file"),
          new AbstractMap.SimpleEntry<>(OutputType.FILES_OPTIONAL, "optional-files"),
          new AbstractMap.SimpleEntry<>(
              OutputType.FILE_WITH_LABELS_OPTIONAL, "optional-file-with-labels"),
          new AbstractMap.SimpleEntry<>(
              OutputType.FILES_WITH_LABELS_OPTIONAL, "optional-files-with-labels"),
          new AbstractMap.SimpleEntry<>(OutputType.LOGS_OPTIONAL, "optional-logs"),
          new AbstractMap.SimpleEntry<>(
              OutputType.QUALITY_CONTROL_OPTIONAL, "optional-quality-control"),
          new AbstractMap.SimpleEntry<>(
              OutputType.WAREHOUSE_RECORDS_OPTIONAL, "optional-warehouse-records"));
  static ObjectMapper MAPPER = new ObjectMapper();

  @BeforeClass
  public static void setUp() {
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

  protected static void serializeTester(String expected, OutputType toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.writeValueAsString(toTest));
    } catch (JsonProcessingException e) {
      Assert.fail("serializeTester threw JsonProcessingException: " + e.getMessage());
    }
  }

  protected static void deserializeTester(OutputType expected, String toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.readValue(toTest, OutputType.class));
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
