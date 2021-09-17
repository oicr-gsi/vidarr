package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Function;
import java.util.stream.Stream;

/** Extract all the values associated with retry values in the input */
public class ExtractRetryValues
    extends BaseInputExtractor<
        Stream<Integer>,
        Stream<Integer>,
        Stream<Integer>,
        Stream<Integer>,
        Stream<Integer>,
        Stream<Integer>> {
  private final ObjectMapper mapper;

  public ExtractRetryValues(ObjectMapper mapper, JsonNode input) {
    super(input);
    this.mapper = mapper;
  }

  @Override
  protected Stream<Integer> aggregateList(Stream<Stream<Integer>> elements) {
    return elements.flatMap(Function.identity());
  }

  @Override
  protected Stream<Integer> aggregateObject(Stream<Stream<Integer>> fields) {
    return fields.flatMap(Function.identity());
  }

  @Override
  protected Stream<Integer> aggregateRetry(Stream<Stream<Integer>> alternatives) {
    return alternatives.flatMap(Function.identity());
  }

  @Override
  protected Stream<Integer> aggregateTuple(Stream<Stream<Integer>> elements) {
    return elements.flatMap(Function.identity());
  }

  @Override
  public Stream<Integer> bool() {
    return Stream.empty();
  }

  @Override
  public Stream<Integer> date() {
    return Stream.empty();
  }

  @Override
  public Stream<Integer> floating() {
    return Stream.empty();
  }

  @Override
  public Stream<Integer> integer() {
    return Stream.empty();
  }

  @Override
  public Stream<Integer> json() {
    return Stream.empty();
  }

  @Override
  protected Stream<Integer> retry(String id, BasicType type, JsonNode value) {
    return Stream.of(Integer.parseUnsignedInt(id));
  }

  @Override
  public Stream<Integer> string() {
    return Stream.empty();
  }

  @Override
  protected Stream<Integer> dictionary(Stream<Stream<Integer>> entries) {
    return entries.flatMap(Function.identity());
  }

  @Override
  protected Stream<Integer> entry(String key, InputType valueType, JsonNode value) {
    return valueType.apply(new ExtractRetryValues(mapper, value));
  }

  @Override
  protected Stream<Integer> entry(
      InputType keyType, JsonNode key, InputType valueType, JsonNode value) {
    return Stream.concat(
        keyType.apply(new ExtractRetryValues(mapper, key)),
        valueType.apply(new ExtractRetryValues(mapper, value)));
  }

  @Override
  protected Stream<Integer> external(
      InputProvisionFormat format, JsonNode input, ExternalId[] externalIds) {
    return Stream.empty();
  }

  @Override
  protected Stream<Integer> internal(InputProvisionFormat format, String id) {
    return Stream.empty();
  }

  @Override
  protected Stream<Integer> list(int index, InputType type, JsonNode value) {
    return type.apply(new ExtractRetryValues(mapper, value));
  }

  @Override
  protected ObjectMapper mapper() {
    return mapper;
  }

  @Override
  protected Stream<Integer> nullValue() {
    return Stream.empty();
  }

  @Override
  protected Stream<Integer> object(String fieldName, InputType type, JsonNode value) {
    return type.apply(new ExtractRetryValues(mapper, value));
  }

  @Override
  protected Stream<Integer> pair(
      InputType left, JsonNode leftValue, InputType right, JsonNode rightValue) {
    return Stream.concat(
        left.apply(new ExtractRetryValues(mapper, leftValue)),
        right.apply(new ExtractRetryValues(mapper, rightValue)));
  }

  @Override
  protected Stream<Integer> tuple(int index, InputType type, JsonNode value) {
    return type.apply(new ExtractRetryValues(mapper, value));
  }

  @Override
  protected Stream<Integer> unionChild(InputType value, JsonNode contents) {
    return value.apply(new ExtractRetryValues(mapper, contents));
  }
}
