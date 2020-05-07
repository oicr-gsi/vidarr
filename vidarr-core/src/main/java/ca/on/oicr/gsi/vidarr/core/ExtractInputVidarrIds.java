package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Function;
import java.util.stream.Stream;

/** Find all Vidarr IDs reference by the input to a workflow run */
public final class ExtractInputVidarrIds
    extends BaseInputExtractor<
        Stream<String>, Stream<String>, Stream<String>, Stream<String>, Stream<String>> {

  private final ObjectMapper mapper;

  public ExtractInputVidarrIds(ObjectMapper mapper, JsonNode arguments) {
    super(arguments);
    this.mapper = mapper;
  }

  @Override
  public Stream<String> bool() {
    return Stream.empty();
  }

  @Override
  public Stream<String> date() {
    return Stream.empty();
  }

  @Override
  protected Stream<String> aggregateList(Stream<Stream<String>> elements) {
    return elements.flatMap(Function.identity());
  }

  @Override
  protected Stream<String> aggregateObject(Stream<Stream<String>> fields) {
    return fields.flatMap(Function.identity());
  }

  @Override
  protected Stream<String> aggregateTuple(Stream<Stream<String>> elements) {
    return elements.flatMap(Function.identity());
  }

  @Override
  protected Stream<String> dictionary(Stream<Stream<String>> entries) {
    return entries.flatMap(Function.identity());
  }

  @Override
  protected Stream<String> entry(String key, InputType valueType, JsonNode value) {
    return valueType.apply(new ExtractInputVidarrIds(mapper, value));
  }

  @Override
  protected Stream<String> entry(
      InputType keyType, JsonNode key, InputType valueType, JsonNode value) {
    return Stream.concat(
        keyType.apply(new ExtractInputVidarrIds(mapper, key)),
        valueType.apply(new ExtractInputVidarrIds(mapper, value)));
  }

  @Override
  protected Stream<String> external(
      InputProvisionFormat format, JsonNode input, ExternalId[] externalIds) {
    return Stream.empty();
  }

  @Override
  protected Stream<String> internal(InputProvisionFormat format, String id) {
    return Stream.of(id);
  }

  @Override
  protected Stream<String> list(int index, InputType type, JsonNode value) {
    return type.apply(new ExtractInputVidarrIds(mapper, value));
  }

  @Override
  public Stream<String> floating() {
    return Stream.empty();
  }

  @Override
  public Stream<String> integer() {
    return Stream.empty();
  }

  @Override
  public Stream<String> json() {
    return Stream.empty();
  }

  @Override
  protected ObjectMapper mapper() {
    return mapper;
  }

  @Override
  protected Stream<String> nullValue() {
    return Stream.empty();
  }

  @Override
  protected Stream<String> object(String key, InputType type, JsonNode value) {
    return type.apply(new ExtractInputVidarrIds(mapper, value));
  }

  @Override
  protected Stream<String> pair(
      InputType left, JsonNode leftValue, InputType right, JsonNode rightValue) {
    return Stream.concat(
        left.apply(new ExtractInputVidarrIds(mapper, leftValue)),
        right.apply(new ExtractInputVidarrIds(mapper, rightValue)));
  }

  @Override
  public Stream<String> string() {
    return Stream.empty();
  }

  @Override
  protected Stream<String> tuple(int index, InputType type, JsonNode value) {
    return type.apply(new ExtractInputVidarrIds(mapper, value));
  }

  @Override
  protected Stream<String> unionChild(InputType value, JsonNode contents) {
    return value.apply(new ExtractInputVidarrIds(mapper, contents));
  }
}
