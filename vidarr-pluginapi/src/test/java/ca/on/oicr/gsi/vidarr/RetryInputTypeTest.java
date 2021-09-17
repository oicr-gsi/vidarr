package ca.on.oicr.gsi.vidarr;


import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

public class RetryInputTypeTest extends InputTypeTest {

  private static final String RETRY_TYPE = "{\"is\":\"retry\",\"inner\":\"%s\"}";

  @Override
  public void testSerialize() {
    for (final var type : BasicTypeTest.primitiveTypes.entrySet()) {
      serializeTester(String.format(RETRY_TYPE, type.getValue()), InputType.retry(type.getKey()));
    }
  }

  @Test
  public void testCreateNullThrows() {
    ThrowingRunnable throwingRunnable = () -> InputType.retry(null);
    Assert.assertThrows(NullPointerException.class, throwingRunnable);
  }

  @Override
  public void testDeserialize() {
    for (final var type : BasicTypeTest.primitiveTypes.entrySet()) {
      deserializeTester(InputType.retry(type.getKey()), String.format(RETRY_TYPE, type.getValue()));
    }
  }

  @Override
  public void testEquals() {
    InputType obj1 = InputType.retry(BasicType.INTEGER),
        obj2 = InputType.retry(BasicType.INTEGER),
        objDifferent = InputType.retry(BasicType.BOOLEAN),
        integer = InputType.INTEGER;

    Assert.assertEquals(obj1, obj1);
    Assert.assertEquals(obj2, obj1);
    Assert.assertNotEquals(integer, obj1);
    Assert.assertNotEquals(null, obj1);
    Assert.assertNotEquals(objDifferent, obj1);
  }
}
