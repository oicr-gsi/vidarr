package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;

public class ListInputTypeTest extends InputTypeTest {
  String listStringFormat = "{\"is\":\"list\",\"inner\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (Map.Entry<InputType, String> type : primitiveTypes.entrySet()) {
      serializeTester(String.format(listStringFormat, type.getValue()), type.getKey().asList());
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<InputType, String> type : primitiveTypes.entrySet()) {
      deserializeTester(type.getKey().asList(), String.format(listStringFormat, type.getValue()));
    }
  }

  @Override
  public void testEquals() {
    InputType list1 = InputType.INTEGER.asList(),
        list2 = InputType.INTEGER.asList(),
        listDifferent = InputType.BOOLEAN.asList(),
        integer = InputType.INTEGER;

    Assert.assertEquals(list1, list1);
    Assert.assertEquals(list2, list1);
    Assert.assertNotEquals(null, list1);
    Assert.assertNotEquals(listDifferent, list1);
    Assert.assertNotEquals(integer, list1);
  }
}
