package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;

/*
  It would be more time-efficient and less code repetition to do:
  for type1 in primitiveTypes:
   for type2 in primitiveTypes:
    test serialize
    test deserialize
  but it'd probably make reporting failures worse
*/
public class DictionaryInputTypeTest extends InputTypeTest {
  String dictAsStringFormat = "{\"is\":\"dictionary\",\"key\":\"%s\",\"value\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (Map.Entry<InputType, String> type1 : primitiveTypes.entrySet()) {
      for (Map.Entry<InputType, String> type2 : primitiveTypes.entrySet()) {
        serializeTester(
            String.format(dictAsStringFormat, type1.getValue(), type2.getValue()),
            InputType.dictionary(type1.getKey(), type2.getKey()));
      }
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<InputType, String> type1 : primitiveTypes.entrySet()) {
      for (Map.Entry<InputType, String> type2 : primitiveTypes.entrySet()) {
        deserializeTester(
            InputType.dictionary(type1.getKey(), type2.getKey()),
            String.format(dictAsStringFormat, type1.getValue(), type2.getValue()));
      }
    }
  }

  // Could be broken into many tests if we want.
  @Override
  public void testEquals() {
    InputType dict1 = InputType.dictionary(InputType.STRING, InputType.INTEGER),
        dict2 = InputType.dictionary(InputType.STRING, InputType.INTEGER),
        dictSameValue = InputType.dictionary(InputType.DATE, InputType.INTEGER),
        dictSameKey = InputType.dictionary(InputType.STRING, InputType.DATE),
        dictDifferent = InputType.dictionary(InputType.DATE, InputType.FILE),
        integer = InputType.INTEGER;

    Assert.assertEquals(dict1, dict1);
    Assert.assertEquals(dict2, dict1);
    Assert.assertNotEquals(integer, dict1);
    Assert.assertNotEquals(null, dict1);
    Assert.assertNotEquals(dictSameKey, dict1);
    Assert.assertNotEquals(dictSameValue, dict1);
    Assert.assertNotEquals(dictDifferent, dict1);
  }
}
