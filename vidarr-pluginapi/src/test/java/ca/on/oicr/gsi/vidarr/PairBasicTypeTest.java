package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;

public class PairBasicTypeTest extends BasicTypeTest {
  String pairAsStringFormat = "{\"is\":\"pair\",\"left\":\"%s\",\"right\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (Map.Entry<BasicType, String> type1 : primitiveTypes.entrySet()) {
      for (Map.Entry<BasicType, String> type2 : primitiveTypes.entrySet()) {
        serializeTester(
            String.format(pairAsStringFormat, type1.getValue(), type2.getValue()),
            BasicType.pair(type1.getKey(), type2.getKey()));
      }
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<BasicType, String> type1 : primitiveTypes.entrySet()) {
      for (Map.Entry<BasicType, String> type2 : primitiveTypes.entrySet()) {
        deserializeTester(
            BasicType.pair(type1.getKey(), type2.getKey()),
            String.format(pairAsStringFormat, type1.getValue(), type2.getValue()));
      }
    }
  }

  // Could be broken into many tests if we want.
  @Override
  public void testEquals() {
    BasicType pair1 = BasicType.pair(BasicType.STRING, BasicType.INTEGER),
        pair2 = BasicType.pair(BasicType.STRING, BasicType.INTEGER),
        pairSameValue = BasicType.pair(BasicType.DATE, BasicType.INTEGER),
        pairSameKey = BasicType.pair(BasicType.STRING, BasicType.DATE),
        pairDifferent = BasicType.pair(BasicType.DATE, BasicType.BOOLEAN),
        pairFlipped = BasicType.pair(BasicType.INTEGER, BasicType.STRING),
        integer = BasicType.INTEGER;

    Assert.assertEquals(pair1, pair1);
    Assert.assertEquals(pair2, pair1);
    Assert.assertNotEquals(integer, pair1);
    Assert.assertNotEquals(null, pair1);
    Assert.assertNotEquals(pairSameKey, pair1);
    Assert.assertNotEquals(pairSameValue, pair1);
    Assert.assertNotEquals(pairDifferent, pair1);
    Assert.assertNotEquals(pairFlipped, pair1);
  }
}
