package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;

public class DictionaryBasicTypeTest extends BasicTypeTest {
  String dictAsStringFormat = "{\"is\":\"dictionary\",\"key\":\"%s\",\"value\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (Map.Entry<BasicType, String> type1 : primitiveTypes.entrySet()) {
      for (Map.Entry<BasicType, String> type2 : primitiveTypes.entrySet()) {
        serializeTester(
            String.format(dictAsStringFormat, type1.getValue(), type2.getValue()),
            BasicType.dictionary(type1.getKey(), type2.getKey()));
      }
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<BasicType, String> type1 : primitiveTypes.entrySet()) {
      for (Map.Entry<BasicType, String> type2 : primitiveTypes.entrySet()) {
        deserializeTester(
            BasicType.dictionary(type1.getKey(), type2.getKey()),
            String.format(dictAsStringFormat, type1.getValue(), type2.getValue()));
      }
    }
  }

  @Override
  public void testEquals() {
    BasicType dict1 = BasicType.dictionary(BasicType.STRING, BasicType.INTEGER),
        dict2 = BasicType.dictionary(BasicType.STRING, BasicType.INTEGER),
        dictSameValue = BasicType.dictionary(BasicType.DATE, BasicType.INTEGER),
        dictSameKey = BasicType.dictionary(BasicType.STRING, BasicType.DATE),
        dictDifferent = BasicType.dictionary(BasicType.DATE, BasicType.BOOLEAN),
        integer = BasicType.INTEGER;

    Assert.assertEquals(dict1, dict1);
    Assert.assertEquals(dict2, dict1);
    Assert.assertNotEquals(integer, dict1);
    Assert.assertNotEquals(null, dict1);
    Assert.assertNotEquals(dictSameKey, dict1);
    Assert.assertNotEquals(dictSameValue, dict1);
    Assert.assertNotEquals(dictDifferent, dict1);
  }
}
