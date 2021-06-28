package ca.on.oicr.gsi.vidarr;

import java.util.Map;
import org.junit.Assert;

public class ListBasicTypeTest extends BasicTypeTest {
  String listStringFormat = "{\"is\":\"list\",\"inner\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (Map.Entry<BasicType, String> type : primitiveTypes.entrySet()) {
      serializeTester(String.format(listStringFormat, type.getValue()), type.getKey().asList());
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<BasicType, String> type : primitiveTypes.entrySet()) {
      deserializeTester(type.getKey().asList(), String.format(listStringFormat, type.getValue()));
    }
  }

  @Override
  public void testEquals() {
    BasicType list1 = BasicType.INTEGER.asList(),
        list2 = BasicType.INTEGER.asList(),
        listDifferent = BasicType.BOOLEAN.asList(),
        integer = BasicType.INTEGER;

    Assert.assertEquals(list1, list1);
    Assert.assertEquals(list2, list1);
    Assert.assertNotEquals(null, list1);
    Assert.assertNotEquals(listDifferent, list1);
    Assert.assertNotEquals(integer, list1);
  }
}
