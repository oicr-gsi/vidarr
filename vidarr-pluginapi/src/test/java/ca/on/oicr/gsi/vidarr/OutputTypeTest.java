package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.OutputType.IdentifierKey;
import java.util.AbstractMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

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
  // override the default max name length of 50,000 for testing because truncating the test data and
  // getting all the expected strings to match is a real pain
  static JsonFactory MAPPER_FACTORY =
      JsonFactory.builder()
          .streamReadConstraints(StreamReadConstraints.builder().maxNameLength(65000).build())
          .build();
  static final JsonMapper MAPPER =
      JsonMapper.builder(MAPPER_FACTORY)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .build();

  protected static void serializeTester(String expected, OutputType toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.writeValueAsString(toTest));
    } catch (JacksonException e) {
      Assert.fail("serializeTester threw JacksonException: " + e.getMessage());
    }
  }

  protected static void deserializeTester(OutputType expected, String toTest) {
    try {
      Assert.assertEquals(expected, MAPPER.readValue(toTest, OutputType.class));
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
