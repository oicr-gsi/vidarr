package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.OutputType.IdentifierKey;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

public class ListOutputTypeTest extends OutputTypeTest {
  final String oneKeyOneOutputStringFormat =
      "{\"is\":\"list\",\"keys\":{\"%s\":\"%s\"},\"outputs\":{\"%s\":\"%s\"}}";

  @Override
  public void testSerialize() {
    for (Map.Entry<OutputType, String> primitiveType : primitiveTypes.entrySet()) {
      for (Map.Entry<IdentifierKey, String> identifierKey : identifierKeys.entrySet()) {
        serializeTester(
            String.format(
                oneKeyOneOutputStringFormat,
                "key",
                identifierKey.getValue(),
                "value",
                primitiveType.getValue()),
            OutputType.list(
                Map.of("key", identifierKey.getKey()), Map.of("value", primitiveType.getKey())));
      }
    }
  }

  @Test
  public void testSerializeMany() {
    Map<String, IdentifierKey> keys = new HashMap<>();
    Map<String, OutputType> values = new HashMap<>();
    String json =
        "{\"is\":\"list\",\"keys\":{\"keya\":\"string\",\"keyb\":\"string\",\"keyc\":\"string\",\"keyd\":\"string\",\"keye\":\"string\",\"keyf\":\"string\",\"keyg\":\"string\",\"keyh\":\"string\",\"keyi\":\"string\",\"keyj\":\"string\",\"keyk\":\"string\",\"keyl\":\"string\",\"keym\":\"string\",\"keyn\":\"string\",\"keyo\":\"string\",\"keyp\":\"string\",\"keyq\":\"string\",\"keyr\":\"string\",\"keys\":\"string\",\"keyt\":\"string\",\"keyu\":\"string\",\"keyv\":\"string\",\"keyw\":\"string\",\"keyx\":\"string\",\"keyy\":\"string\",\"keyz\":\"string\"},\"outputs\":{\"valuev\":\"file\",\"valueu\":\"file\",\"valuex\":\"file\",\"valuew\":\"file\",\"valuer\":\"file\",\"valueq\":\"file\",\"valuet\":\"file\",\"values\":\"file\",\"valuez\":\"file\",\"valuey\":\"file\",\"valuef\":\"file\",\"valuee\":\"file\",\"valueh\":\"file\",\"valueg\":\"file\",\"valueb\":\"file\",\"valuea\":\"file\",\"valued\":\"file\",\"valuec\":\"file\",\"valuen\":\"file\",\"valuem\":\"file\",\"valuep\":\"file\",\"valueo\":\"file\",\"valuej\":\"file\",\"valuei\":\"file\",\"valuel\":\"file\",\"valuek\":\"file\"}}";

    for (char c = 'a'; c <= 'z'; c++) {
      keys.put("key" + c, IdentifierKey.STRING);
      values.put("value" + c, OutputType.FILE);
    }

    serializeTester(json, OutputType.list(keys, values));
  }

  @Test
  public void testSerializeEmoji() {
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "💔", "string", "value", "file"),
        OutputType.list(Map.of("💔", IdentifierKey.STRING), Map.of("value", OutputType.FILE)));
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "key", "string", "💔", "file"),
        OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("💔", OutputType.FILE)));
    // Test that emoji don't cause overlap issues
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "✌️", "string", "💔", "file"),
        OutputType.list(Map.of("✌️", IdentifierKey.STRING), Map.of("💔", OutputType.FILE)));
    // Test that skintone modifiers don't cause overlap issues
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "✌🏻", "string", "✌🏿", "file"),
        OutputType.list(Map.of("✌🏻", IdentifierKey.STRING), Map.of("✌🏿", OutputType.FILE)));
  }

  @Test
  public void testSerializeNull() {
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "null", "string", "value", "file"),
        OutputType.list(Map.of("null", IdentifierKey.STRING), Map.of("value", OutputType.FILE)));
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "key", "string", "null", "file"),
        OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("null", OutputType.FILE)));
  }

  @Test
  public void testSerializeStringBool() {
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "true", "string", "value", "file"),
        OutputType.list(Map.of("true", IdentifierKey.STRING), Map.of("value", OutputType.FILE)));
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "key", "string", "true", "file"),
        OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("true", OutputType.FILE)));
  }

  @Test
  public void testEmptyStringThrows() {
    Assert.fail("Not yet implemented.");
  }

  @Test
  public void testSerializeInterpolation() {
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "$HOME", "string", "value", "file"),
        OutputType.list(Map.of("$HOME", IdentifierKey.STRING), Map.of("value", OutputType.FILE)));
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "key", "string", "$HOME", "file"),
        OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("$HOME", OutputType.FILE)));
  }

  @Test
  public void testSerializeOneLongString() {
    Assert.fail("Not yet implemented.");
  }

  @Test
  public void testSerializeCaseSensitive() {
    serializeTester(
        String.format(oneKeyOneOutputStringFormat, "field", "string", "Field", "file"),
        OutputType.list(Map.of("field", IdentifierKey.STRING), Map.of("Field", OutputType.FILE)));
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<OutputType, String> primitiveType : primitiveTypes.entrySet()) {
      for (Map.Entry<IdentifierKey, String> identifierKey : identifierKeys.entrySet()) {
        deserializeTester(
            OutputType.list(
                Map.of("key", identifierKey.getKey()), Map.of("value", primitiveType.getKey())),
            String.format(
                oneKeyOneOutputStringFormat,
                "key",
                identifierKey.getValue(),
                "value",
                primitiveType.getValue()));
      }
    }
  }

  @Test
  public void testDeserializeMany() {
    Map<String, IdentifierKey> keys = new HashMap<>();
    Map<String, OutputType> values = new HashMap<>();
    String json =
        "{\"is\":\"list\",\"keys\":{\"keya\":\"string\",\"keyb\":\"string\",\"keyc\":\"string\",\"keyd\":\"string\",\"keye\":\"string\",\"keyf\":\"string\",\"keyg\":\"string\",\"keyh\":\"string\",\"keyi\":\"string\",\"keyj\":\"string\",\"keyk\":\"string\",\"keyl\":\"string\",\"keym\":\"string\",\"keyn\":\"string\",\"keyo\":\"string\",\"keyp\":\"string\",\"keyq\":\"string\",\"keyr\":\"string\",\"keys\":\"string\",\"keyt\":\"string\",\"keyu\":\"string\",\"keyv\":\"string\",\"keyw\":\"string\",\"keyx\":\"string\",\"keyy\":\"string\",\"keyz\":\"string\"},\"outputs\":{\"valuev\":\"file\",\"valueu\":\"file\",\"valuex\":\"file\",\"valuew\":\"file\",\"valuer\":\"file\",\"valueq\":\"file\",\"valuet\":\"file\",\"values\":\"file\",\"valuez\":\"file\",\"valuey\":\"file\",\"valuef\":\"file\",\"valuee\":\"file\",\"valueh\":\"file\",\"valueg\":\"file\",\"valueb\":\"file\",\"valuea\":\"file\",\"valued\":\"file\",\"valuec\":\"file\",\"valuen\":\"file\",\"valuem\":\"file\",\"valuep\":\"file\",\"valueo\":\"file\",\"valuej\":\"file\",\"valuei\":\"file\",\"valuel\":\"file\",\"valuek\":\"file\"}}";

    for (char c = 'a'; c <= 'z'; c++) {
      keys.put("key" + c, IdentifierKey.STRING);
      values.put("value" + c, OutputType.FILE);
    }

    deserializeTester(OutputType.list(keys, values), json);
  }

  @Test
  public void testDeserializeEmoji() {
    deserializeTester(
        OutputType.list(Map.of("💔", IdentifierKey.STRING), Map.of("value", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "💔", "string", "value", "file"));
    deserializeTester(
        OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("💔", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "key", "string", "💔", "file"));
    // Test that emoji don't cause overlap issues
    deserializeTester(
        OutputType.list(Map.of("✌️", IdentifierKey.STRING), Map.of("💔", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "✌️", "string", "💔", "file"));
    // Test that skintone modifiers don't cause overlap issues
    deserializeTester(
        OutputType.list(Map.of("✌🏻", IdentifierKey.STRING), Map.of("✌🏿", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "✌🏻", "string", "✌🏿", "file"));
  }

  @Test
  public void testDeserializeNull() {
    deserializeTester(
        OutputType.list(Map.of("null", IdentifierKey.STRING), Map.of("value", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "null", "string", "value", "file"));
    deserializeTester(
        OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("null", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "key", "string", "null", "file"));
  }

  @Test
  public void testDeserializeStringBool() {
    deserializeTester(
        OutputType.list(Map.of("true", IdentifierKey.STRING), Map.of("value", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "true", "string", "value", "file"));
    deserializeTester(
        OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("true", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "key", "string", "true", "file"));
  }

  @Test
  public void testDeserializeInterpolation() {
    deserializeTester(
        OutputType.list(Map.of("$HOME", IdentifierKey.STRING), Map.of("value", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "$HOME", "string", "value", "file"));
    deserializeTester(
        OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("$HOME", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "key", "string", "$HOME", "file"));
  }

  @Test
  public void testDeserializeOneLongString() {
    Assert.fail("Not yet implemented.");
  }

  @Test
  public void testDeserializeCaseSensitive() {
    deserializeTester(
        OutputType.list(Map.of("field", IdentifierKey.STRING), Map.of("Field", OutputType.FILE)),
        String.format(oneKeyOneOutputStringFormat, "field", "string", "Field", "file"));
  }

  @Test
  public void testCreateOverlapThrows() {
    ThrowingRunnable throwingRunnable1 =
        () -> OutputType.list(Map.of("bad", IdentifierKey.STRING), Map.of("bad", OutputType.FILE));
    ThrowingRunnable throwingRunnable2 =
        () ->
            OutputType.list(
                Map.of("ok", IdentifierKey.STRING, "bad", IdentifierKey.STRING),
                Map.of("bad", OutputType.FILE));
    ThrowingRunnable throwingRunnable3 =
        () ->
            OutputType.list(
                Map.of("bad", IdentifierKey.STRING),
                Map.of("ok", OutputType.FILE, "bad", OutputType.FILE));
    Assert.assertThrows(IllegalArgumentException.class, throwingRunnable1);
    Assert.assertThrows(IllegalArgumentException.class, throwingRunnable2);
    Assert.assertThrows(IllegalArgumentException.class, throwingRunnable3);
  }

  @Test
  public void testCreateNullThrows() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> OutputType.list(null, Map.of("value", OutputType.FILE)));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> OutputType.list(Map.of("key", IdentifierKey.STRING), null));
  }

  @Test
  public void testCreateEmptyThrows() {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> OutputType.list(Map.of(), Map.of("value", OutputType.FILE)));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of()));
  }

  @Test
  public void testCreateMapOfNullThrows() {
    // TODO: should a better exception than NPE get thrown?
    Assert.assertThrows(
        NullPointerException.class,
        () ->
            OutputType.list(Map.of(null, IdentifierKey.STRING), Map.of("value", OutputType.FILE)));
    Assert.assertThrows(
        NullPointerException.class,
        () -> OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of(null, OutputType.FILE)));
    Assert.assertThrows(
        NullPointerException.class,
        () -> OutputType.list(Map.of("key", null), Map.of("value", OutputType.FILE)));
    Assert.assertThrows(
        NullPointerException.class,
        () -> OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("value", null)));
  }

  @Override
  public void testEquals() {
    OutputType
        list1 =
            OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("value", OutputType.FILE)),
        list2 =
            OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("value", OutputType.FILE)),
        listDifferentKey =
            OutputType.list(Map.of("key", IdentifierKey.INTEGER), Map.of("value", OutputType.FILE)),
        listDifferentValue =
            OutputType.list(Map.of("key", IdentifierKey.STRING), Map.of("value", OutputType.LOGS)),
        listMultiple1 =
            OutputType.list(
                Map.of("key", IdentifierKey.STRING, "key2", IdentifierKey.STRING),
                Map.of("value", OutputType.FILE, "value2", OutputType.FILE)),
        listMultiple2 =
            OutputType.list(
                Map.of("key", IdentifierKey.STRING, "key2", IdentifierKey.STRING),
                Map.of("value", OutputType.FILE, "value2", OutputType.FILE)),
        listMultipleDifferentKeys =
            OutputType.list(
                Map.of("key", IdentifierKey.STRING, "key2", IdentifierKey.INTEGER),
                Map.of("value", OutputType.FILE, "value2", OutputType.FILE)),
        listMultipleDifferentValues =
            OutputType.list(
                Map.of("key", IdentifierKey.STRING, "key2", IdentifierKey.STRING),
                Map.of("value", OutputType.FILE, "value2", OutputType.LOGS)),
        file = OutputType.FILE;

    Assert.assertEquals(list1, list1);
    Assert.assertEquals(list2, list1);
    Assert.assertEquals(listMultiple1, listMultiple1);
    Assert.assertEquals(listMultiple2, listMultiple1);
    Assert.assertNotEquals(listDifferentKey, list1);
    Assert.assertNotEquals(listDifferentValue, list1);
    Assert.assertNotEquals(listMultiple1, list1);
    Assert.assertNotEquals(file, list1);
    Assert.assertNotEquals(listMultipleDifferentKeys, listMultiple1);
    Assert.assertNotEquals(listMultipleDifferentValues, listMultiple1);
  }
}
