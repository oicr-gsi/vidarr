package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

/** Tuples can have any number of elements. */
public class TupleBasicTypeTest extends BasicTypeTest {
  // For this set of tests, elements list will need to be built outside of format
  String tupleStringFormat = "{\"is\":\"tuple\",\"elements\":[%s]}";

  @Override
  public void testSerialize() {
    for (Map.Entry<BasicType, String> type : primitiveTypes.entrySet()) {
      serializeTester(
          String.format(tupleStringFormat, "\"" + type.getValue() + "\""),
          BasicType.tuple(type.getKey()));
    }
  }

  @Test
  public void testSerializeMany() {
    List<BasicType> varargs = new LinkedList<>();
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode types = MAPPER.createArrayNode();
    root.put("is", "tuple");
    String json = "";

    for (int i = 0; i < 100; i++) {
      varargs.add(BasicType.BOOLEAN);
      types.add(MAPPER.convertValue("boolean", JsonNode.class));
      root.set("elements", types);
      try {
        json = MAPPER.writeValueAsString(root);
      } catch (JsonProcessingException e) {
        Assert.fail("JsonProcessingException writing long json: " + e.getMessage());
      }
      serializeTester(json, BasicType.tuple(varargs.toArray(new BasicType[varargs.size()])));
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<BasicType, String> type : primitiveTypes.entrySet()) {
      deserializeTester(
          BasicType.tuple(type.getKey()),
          String.format(tupleStringFormat, "\"" + type.getValue() + "\""));
    }
  }

  @Test
  public void testDeserializeMany() {
    List<BasicType> varargs = new LinkedList<>();
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode types = MAPPER.createArrayNode();
    root.put("is", "tuple");
    String json = "";

    for (int i = 0; i < 100; i++) {
      varargs.add(BasicType.BOOLEAN);
      types.add(MAPPER.convertValue("boolean", JsonNode.class));
      root.set("elements", types);
      try {
        json = MAPPER.writeValueAsString(root);
      } catch (JsonProcessingException e) {
        Assert.fail("JsonProcessingException writing long json: " + e.getMessage());
      }
      deserializeTester(BasicType.tuple(varargs.toArray(new BasicType[varargs.size()])), json);
    }
  }

  @Test
  public void testCreateNullThrows() {
    ThrowingRunnable throwingRunnable = () -> BasicType.tuple(null);
    Assert.assertThrows(NullPointerException.class, throwingRunnable);
  }

  @Test
  public void testCreateTwoNullsThrows() {
    ThrowingRunnable throwingRunnable = () -> BasicType.tuple(null, null);
    Assert.assertThrows(NullPointerException.class, throwingRunnable);
  }

  @Test
  public void testCreateNullTypeThrows() {
    ThrowingRunnable throwingRunnable =
        () -> BasicType.tuple(BasicType.DATE, null, BasicType.BOOLEAN);
    Assert.assertThrows(NullPointerException.class, throwingRunnable);
  }

  @Override
  public void testEquals() {
    BasicType tuple1 = BasicType.tuple(BasicType.STRING, BasicType.INTEGER),
        tuple2 = BasicType.tuple(BasicType.STRING, BasicType.INTEGER),
        tupleSubset = BasicType.tuple(BasicType.STRING),
        tupleSuperset = BasicType.tuple(BasicType.STRING, BasicType.INTEGER, BasicType.BOOLEAN),
        tupleDifferent = BasicType.tuple(BasicType.BOOLEAN, BasicType.DATE),
        tupleFlipped = BasicType.tuple(BasicType.INTEGER, BasicType.STRING),
        integer = BasicType.INTEGER;

    Assert.assertEquals(tuple1, tuple1);
    Assert.assertEquals(tuple2, tuple1);
    Assert.assertNotEquals(tupleSubset, tuple1);
    Assert.assertNotEquals(tupleSuperset, tuple1);
    Assert.assertNotEquals(tupleDifferent, tuple1);
    Assert.assertNotEquals(tupleFlipped, tuple1);
    Assert.assertNotEquals(integer, tuple1);
  }
}
