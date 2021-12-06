package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Function;
import java.util.stream.Stream;

/** Fina all external IDs referenced by the input */
public final class ExtractInputExternalIds
    extends BaseInputExtractor<
        Stream<? extends ExternalId>,
        Stream<? extends ExternalId>,
        Stream<? extends ExternalId>,
        Stream<? extends ExternalId>,
        Stream<? extends ExternalId>> {

  private final ObjectMapper mapper;
  private final FileResolver fileResolver;

  public ExtractInputExternalIds(
      ObjectMapper mapper, JsonNode arguments, FileResolver fileResolver) {
    super(arguments);
    this.mapper = mapper;
    this.fileResolver = fileResolver;
  }

  @Override
  public Stream<? extends ExternalId> bool() {
    return Stream.empty();
  }

  @Override
  public Stream<? extends ExternalId> date() {
    return Stream.empty();
  }

  @Override
  protected Stream<? extends ExternalId> aggregateList(
      Stream<Stream<? extends ExternalId>> elements) {
    return elements.flatMap(Function.identity());
  }

  @Override
  protected Stream<? extends ExternalId> aggregateObject(
      Stream<Stream<? extends ExternalId>> fields) {
    return fields.flatMap(Function.identity());
  }

  @Override
  protected Stream<? extends ExternalId> aggregateTuple(
      Stream<Stream<? extends ExternalId>> elements) {
    return elements.flatMap(Function.identity());
  }

  @Override
  protected Stream<? extends ExternalId> dictionary(Stream<Stream<? extends ExternalId>> entries) {
    return entries.flatMap(Function.identity());
  }

  @Override
  protected Stream<? extends ExternalId> entry(String key, InputType valueType, JsonNode value) {
    return valueType.apply(new ExtractInputExternalIds(mapper, value, fileResolver));
  }

  @Override
  protected Stream<? extends ExternalId> entry(
      InputType keyType, JsonNode key, InputType valueType, JsonNode value) {
    return Stream.concat(
        keyType.apply(new ExtractInputExternalIds(mapper, key, fileResolver)),
        valueType.apply(new ExtractInputExternalIds(mapper, value, fileResolver)));
  }

  @Override
  protected Stream<? extends ExternalId> external(
      InputProvisionFormat format, JsonNode input, ExternalId[] externalIds) {
    return Stream.of(externalIds);
  }

  @Override
  protected Stream<? extends ExternalId> internal(InputProvisionFormat format, String id) {
    return fileResolver
        .pathForId(id)
        .map(FileMetadata::externalKeys)
        .orElseThrow(String.format("Could not resolve internal ID %s", id));
  }

  @Override
  protected Stream<? extends ExternalId> list(int index, InputType type, JsonNode value) {
    return type.apply(new ExtractInputExternalIds(mapper, value, fileResolver));
  }

  @Override
  public Stream<? extends ExternalId> floating() {
    return Stream.empty();
  }

  @Override
  public Stream<? extends ExternalId> integer() {
    return Stream.empty();
  }

  @Override
  public Stream<? extends ExternalId> json() {
    return Stream.empty();
  }

  @Override
  protected ObjectMapper mapper() {
    return mapper;
  }

  @Override
  protected Stream<? extends ExternalId> nullValue() {
    return Stream.empty();
  }

  @Override
  protected Stream<? extends ExternalId> object(String key, InputType type, JsonNode value) {
    return type.apply(new ExtractInputExternalIds(mapper, value, fileResolver));
  }

  @Override
  protected Stream<? extends ExternalId> pair(
      InputType left, JsonNode leftValue, InputType right, JsonNode rightValue) {
    return Stream.concat(
        left.apply(new ExtractInputExternalIds(mapper, leftValue, fileResolver)),
        right.apply(new ExtractInputExternalIds(mapper, rightValue, fileResolver)));
  }

  @Override
  public Stream<? extends ExternalId> string() {
    return Stream.empty();
  }

  @Override
  protected Stream<? extends ExternalId> tuple(int index, InputType type, JsonNode value) {
    return type.apply(new ExtractInputExternalIds(mapper, value, fileResolver));
  }

  @Override
  protected Stream<? extends ExternalId> unionChild(InputType value, JsonNode contents) {
    return value.apply(new ExtractInputExternalIds(mapper, contents, fileResolver));
  }
}
