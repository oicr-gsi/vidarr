package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Maraud over the input parameters
 *
 * @param <R> the result type
 * @param <D> the type from each entry in a dictionary
 * @param <E> the type from each element in a tuple
 * @param <F> the type from each field in an object
 * @param <L> the type from each element in a list
 */
abstract class BaseInputExtractor<R, D, E, F, L> implements InputType.Visitor<R> {

  public static final String EXTERNAL__IDS = "externalIds";
  public static final String EXTERNAL__CONFIG = "configuration";
  private final JsonNode input;

  public BaseInputExtractor(JsonNode input) {
    this.input = input;
  }

  protected abstract R aggregateList(Stream<L> elements);

  protected abstract R aggregateObject(Stream<F> fields);

  protected abstract R aggregateTuple(Stream<E> elements);

  @Override
  public final R dictionary(InputType key, InputType value) {
    if (input.isObject() && key == InputType.STRING) {
      return dictionary(
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(input.fields(), 0), false)
              .map(e -> entry(e.getKey(), value, e.getValue())));
    } else if (input.isArray()) {
      return dictionary(
          IntStream.range(0, input.size())
              .mapToObj(
                  i -> {
                    final var inputEntry = input.get(i);
                    if (!inputEntry.isArray() || inputEntry.size() != 2) {
                      throw new IllegalArgumentException();
                    }
                    return entry(key, inputEntry.get(0), value, inputEntry.get(1));
                  }));
    } else {
      throw new IllegalArgumentException();
    }
  }

  protected abstract R dictionary(Stream<D> entries);

  @Override
  public final R directory() {
    return handle(InputProvisionFormat.DIRECTORY);
  }

  protected abstract D entry(String key, InputType valueType, JsonNode value);

  protected abstract D entry(InputType keyType, JsonNode key, InputType valueType, JsonNode value);

  protected abstract R external(
      InputProvisionFormat format, JsonNode input, ExternalId[] externalIds);

  @Override
  public final R file() {
    return handle(InputProvisionFormat.FILE);
  }

  private R handle(InputProvisionFormat format) {
    if (input.isObject()
        && input.has("contents")
        && input.has("type")
        && input.get("type").isTextual()) {
      final var contents = input.get("contents");
      switch (input.get("type").asText()) {
        case "EXTERNAL":
          if (!contents.isObject() || !contents.has(EXTERNAL__IDS)) {
            throw new IllegalArgumentException();
          }
          final var externalIds = contents.get(EXTERNAL__IDS);
          if (!externalIds.isArray()) {
            throw new IllegalArgumentException();
          }
          return external(
              format,
              contents.get(EXTERNAL__CONFIG),
              mapper().treeToValue(externalIds, ExternalId[].class));
        case "INTERNAL":
          if (contents.isArray() && contents.size() == 1 && contents.get(0).isTextual()) {
            return internal(format, contents.get(0).asText());
          } else {
            throw new IllegalArgumentException("Invalid input file for INTERNAL");
          }
      }
    }
    throw new IllegalArgumentException();
  }

  protected abstract R internal(InputProvisionFormat format, String id);

  @Override
  public final R list(InputType inner) {
    if (input.isArray()) {
      return aggregateList(
          IntStream.range(0, input.size()).mapToObj(i -> list(i, inner, input.get(i))));
    } else {
      throw new IllegalArgumentException();
    }
  }

  protected abstract L list(int index, InputType type, JsonNode value);

  protected abstract ObjectMapper mapper();

  protected abstract R nullValue();

  @Override
  public final R object(Stream<Pair<String, InputType>> contents) {
    if (input.isObject()) {
      return aggregateObject(
          contents.map(p -> object(p.first(), p.second(), input.get(p.first()))));
    } else {
      contents.close();
      throw new IllegalArgumentException();
    }
  }

  protected abstract F object(String fieldName, InputType type, JsonNode value);

  @Override
  public final R optional(InputType inner) {
    if (input.isNull()) {
      return nullValue();
    } else {
      return inner.apply(this);
    }
  }

  protected abstract R pair(
      InputType left, JsonNode leftValue, InputType right, JsonNode rightValue);

  @Override
  public final R pair(InputType left, InputType right) {
    if (input.isArray()) {
      if (input.size() == 2) {
        return pair(left, input.get(0), right, input.get(1));
      } else {
        throw new IllegalArgumentException("Pair with incorrect number of arguments");
      }
    } else if (input.isObject()) {
      if (input.has("left") && input.has("right")) {
        return pair(left, input.get("left"), right, input.get("right"));

      } else {
        throw new IllegalArgumentException("Pair with incorrect fields");
      }

    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public final R taggedUnion(Stream<Map.Entry<String, InputType>> elements) {
    if (input.isObject() && input.get("type").isTextual()) {
      final var type = input.get("type").asText();
      return elements
          .filter(e -> e.getKey().equals(type))
          .findFirst()
          .map(e -> unionChild(e.getValue(), input.get("contents")))
          .orElseThrow();
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public final R tuple(Stream<InputType> contents) {
    if (input.isArray()) {
      return aggregateTuple(
          contents.map(
              new Function<>() {
                private int index;

                @Override
                public E apply(InputType t) {
                  return tuple(index, t, input.get(index++));
                }
              }));
    } else {
      contents.close();
      throw new IllegalArgumentException();
    }
  }

  protected abstract E tuple(int index, InputType type, JsonNode value);

  protected abstract R unionChild(InputType value, JsonNode contents);
}
