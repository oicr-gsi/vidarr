package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;

public class OptionalInputTypeTest extends InputTypeTest {
  String optionalStringFormat = "{\"is\":\"optional\",\"inner\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (Map.Entry<InputType, String> type : primitiveTypes.entrySet()) {
      serializeTester(
          String.format(optionalStringFormat, type.getValue()), type.getKey().asOptional());
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<InputType, String> type : primitiveTypes.entrySet()) {
      deserializeTester(
          type.getKey().asOptional(), String.format(optionalStringFormat, type.getValue()));
    }
  }

  @Override
  public void testEquals() {
    InputType opt1 = InputType.INTEGER.asOptional(),
        opt2 = InputType.INTEGER.asOptional(),
        optDifferent = InputType.BOOLEAN.asOptional(),
        integer = InputType.INTEGER;

    Assert.assertEquals(opt1, opt1);
    Assert.assertEquals(opt2, opt1);
    Assert.assertNotEquals(null, opt1);
    Assert.assertNotEquals(optDifferent, opt1);
    Assert.assertNotEquals(integer, opt1);
  }
}
