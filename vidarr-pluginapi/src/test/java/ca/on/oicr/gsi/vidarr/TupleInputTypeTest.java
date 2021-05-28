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
public class TupleInputTypeTest extends InputTypeTest {
  // For this set of tests, elements list will need to be built outside of format
  String tupleStringFormat = "{\"is\":\"tuple\",\"elements\":[%s]}";

  @Override
  public void testSerialize() {
    for (Map.Entry<InputType, String> type : primitiveTypes.entrySet()) {
      serializeTester(
          String.format(tupleStringFormat, "\"" + type.getValue() + "\""),
          InputType.tuple(type.getKey()));
    }
  }

  @Test
  public void testSerializeMany() {
    List<InputType> varargs = new LinkedList<>();
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode types = MAPPER.createArrayNode();
    root.set("is", MAPPER.convertValue("tuple", JsonNode.class));
    String json = "";

    for (int i = 0; i < 100; i++) {
      varargs.add(InputType.BOOLEAN);
      types.add(MAPPER.convertValue("boolean", JsonNode.class));
      root.set("elements", types);
      try {
        json = MAPPER.writeValueAsString(root);
      } catch (JsonProcessingException e) {
        Assert.fail("JsonProcessingException writing long json: " + e.getMessage());
      }
      serializeTester(json, InputType.tuple(varargs.toArray(new InputType[varargs.size()])));
    }
  }

  @Override
  public void testDeserialize() {
    for (Map.Entry<InputType, String> type : primitiveTypes.entrySet()) {
      deserializeTester(
          InputType.tuple(type.getKey()),
          String.format(tupleStringFormat, "\"" + type.getValue() + "\""));
    }
  }

  @Test
  public void testDeserializeMany() {
    List<InputType> varargs = new LinkedList<>();
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode types = MAPPER.createArrayNode();
    root.set("is", MAPPER.convertValue("tuple", JsonNode.class));
    String json = "";

    for (int i = 0; i < 100; i++) {
      varargs.add(InputType.BOOLEAN);
      types.add(MAPPER.convertValue("boolean", JsonNode.class));
      root.set("elements", types);
      try {
        json = MAPPER.writeValueAsString(root);
      } catch (JsonProcessingException e) {
        Assert.fail("JsonProcessingException writing long json: " + e.getMessage());
      }
      deserializeTester(InputType.tuple(varargs.toArray(new InputType[varargs.size()])), json);
    }
  }

  @Test
  public void testCreateNullThrows() {
    ThrowingRunnable throwingRunnable = () -> InputType.tuple(null);
    Assert.assertThrows(NullPointerException.class, throwingRunnable);
  }

  @Test
  public void testCreateTwoNullsThrows() {
    ThrowingRunnable throwingRunnable = () -> InputType.tuple(null, null);
    Assert.assertThrows(NullPointerException.class, throwingRunnable);
  }

  @Test
  public void testCreateNullTypeThrows() {
    ThrowingRunnable throwingRunnable = () -> InputType.tuple(InputType.DATE, null, InputType.FILE);
    Assert.assertThrows(NullPointerException.class, throwingRunnable);
  }

  @Override
  public void testEquals() {
    InputType tuple1 = InputType.tuple(InputType.STRING, InputType.INTEGER),
        tuple2 = InputType.tuple(InputType.STRING, InputType.INTEGER),
        tupleSubset = InputType.tuple(InputType.STRING),
        tupleSuperset = InputType.tuple(InputType.STRING, InputType.INTEGER, InputType.FILE),
        tupleDifferent = InputType.tuple(InputType.FILE, InputType.DATE),
        tupleFlipped = InputType.tuple(InputType.INTEGER, InputType.STRING),
        integer = InputType.INTEGER;

    Assert.assertEquals(tuple1, tuple1);
    Assert.assertEquals(tuple2, tuple1);
    Assert.assertNotEquals(tupleSubset, tuple1);
    Assert.assertNotEquals(tupleSuperset, tuple1);
    Assert.assertNotEquals(tupleDifferent, tuple1);
    Assert.assertNotEquals(tupleFlipped, tuple1);
    Assert.assertNotEquals(integer, tuple1);
  }
}
