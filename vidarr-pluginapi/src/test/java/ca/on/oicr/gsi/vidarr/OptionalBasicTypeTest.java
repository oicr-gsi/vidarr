package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;

public class OptionalBasicTypeTest extends BasicTypeTest {
  String optionalStringFormat = "{\"is\":\"optional\",\"inner\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (Map.Entry<BasicType, String> type : primitiveTypes.entrySet()) {
      serializeTester(
          String.format(optionalStringFormat, type.getValue()), type.getKey().asOptional());
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<BasicType, String> type : primitiveTypes.entrySet()) {
      deserializeTester(
          type.getKey().asOptional(), String.format(optionalStringFormat, type.getValue()));
    }
  }

  @Override
  public void testEquals() {
    BasicType opt1 = BasicType.INTEGER.asOptional(),
        opt2 = BasicType.INTEGER.asOptional(),
        optDifferent = BasicType.BOOLEAN.asOptional(),
        integer = BasicType.INTEGER;

    Assert.assertEquals(opt1, opt1);
    Assert.assertEquals(opt2, opt1);
    Assert.assertNotEquals(null, opt1);
    Assert.assertNotEquals(optDifferent, opt1);
    Assert.assertNotEquals(integer, opt1);
  }
}
