package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;

// TODO: this is c&p'd from Dictionary&c. Reduce repetition.
public class PairInputTypeTest extends InputTypeTest {
  String pairAsStringFormat = "{\"is\":\"pair\",\"left\":\"%s\",\"right\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (Map.Entry<InputType, String> type1 : primitiveTypes.entrySet()) {
      for (Map.Entry<InputType, String> type2 : primitiveTypes.entrySet()) {
        serializeTester(
            String.format(pairAsStringFormat, type1.getValue(), type2.getValue()),
            InputType.pair(type1.getKey(), type2.getKey()));
      }
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<InputType, String> type1 : primitiveTypes.entrySet()) {
      for (Map.Entry<InputType, String> type2 : primitiveTypes.entrySet()) {
        deserializeTester(
            InputType.pair(type1.getKey(), type2.getKey()),
            String.format(pairAsStringFormat, type1.getValue(), type2.getValue()));
      }
    }
  }

  // Could be broken into many tests if we want.
  @Override
  public void testEquals() {
    InputType pair1 = InputType.pair(InputType.STRING, InputType.INTEGER),
        pair2 = InputType.pair(InputType.STRING, InputType.INTEGER),
        pairSameValue = InputType.pair(InputType.DATE, InputType.INTEGER),
        pairSameKey = InputType.pair(InputType.STRING, InputType.DATE),
        pairDifferent = InputType.pair(InputType.DATE, InputType.FILE),
        pairFlipped = InputType.pair(InputType.INTEGER, InputType.STRING),
        integer = InputType.INTEGER;

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
