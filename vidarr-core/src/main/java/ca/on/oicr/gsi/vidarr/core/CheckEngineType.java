package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Check the provided JSON matches an engine type
 *
 * <p>The result will be a stream of errors; if empty, no errors were found.
 */
public final class CheckEngineType implements BasicType.Visitor<Stream<String>> {
  private final JsonNode input;

  public CheckEngineType(JsonNode input) {
    this.input = input;
  }

  @Override
  public Stream<String> bool() {
    return input.isBoolean()
        ? Stream.empty()
        : Stream.of("Expected Boolean but got " + input.toPrettyString());
  }

  @Override
  public Stream<String> date() {
    if (input.isTextual()) {
      try {
        DateTimeFormatter.ISO_INSTANT.parse(input.asText());
        return Stream.empty();
      } catch (DateTimeParseException e) {
        // Do nothing
      }
    }
    return Stream.of("Expected date but got " + input.toPrettyString());
  }

  @Override
  public Stream<String> dictionary(BasicType key, BasicType value) {
    if (input.isObject() && key == BasicType.STRING) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(v -> value.apply(new CheckEngineType(v)));
    }
    if (input.isArray()) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(
              e ->
                  e.isArray() && e.size() == 2
                      ? Stream.of(
                          "Expected inner key as array with two elements, but got "
                              + e.toPrettyString())
                      : Stream.concat(
                          key.apply(new CheckEngineType(e.get(0))),
                          value.apply(new CheckEngineType(e.get(1)))));
    } else {
      return Stream.of("Expected dictionary, but got " + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> floating() {
    return input.isFloatingPointNumber()
        ? Stream.empty()
        : Stream.of("Expected float but got " + input.toPrettyString());
  }

  @Override
  public Stream<String> integer() {
    return input.isIntegralNumber()
        ? Stream.empty()
        : Stream.of("Expected integer but got " + input.toPrettyString());
  }

  @Override
  public Stream<String> json() {
    return Stream.empty();
  }

  @Override
  public Stream<String> list(BasicType inner) {
    if (input.isArray()) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(e -> inner.apply(new CheckEngineType(e)));
    } else {
      return Stream.of("Expected list (as array), but got " + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> object(Stream<Pair<String, BasicType>> contents) {
    if (input.isObject()) {
      return contents.flatMap(
          p ->
              input.has(p.first())
                  ? p.second().apply(new CheckEngineType(input.get(p.first())))
                  : Stream.of("Missing attribute " + p.first() + " in object"));
    } else {
      return Stream.of("Expected object, but got " + input.getNodeType().name());
    }
  }

  @Override
  public Stream<String> taggedUnion(Stream<Map.Entry<String, BasicType>> elements) {
    if (input.isObject() && input.has("type") && input.has("contents")) {
      final var type = input.get("type");
      if (type.isTextual()) {
        final var typeStr = type.asText();
        return elements
            .filter(e -> e.getKey().equals(typeStr))
            .findAny()
            .map(e -> e.getValue().apply(new CheckEngineType(input.get("contents"))))
            .orElseGet(() -> Stream.of("Unkown type in tagged union: " + type.asText()));
      } else {
        return Stream.of("Expected string type in tagged union, but got " + type.toPrettyString());
      }
    } else {
      return Stream.of("Expected tagged union (as object), but got " + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> optional(BasicType inner) {
    return input.isNull() ? Stream.empty() : inner.apply(this);
  }

  @Override
  public Stream<String> pair(BasicType left, BasicType right) {
    if (input.isObject() && input.has("left") && input.has("right")) {
      return Stream.concat(
          left.apply(new CheckEngineType(input.get("left"))),
          right.apply(new CheckEngineType(input.get("right"))));
    } else {
      return Stream.of(
          "Expected pair (as object with left and right), but got " + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> string() {
    return input.isTextual()
        ? Stream.empty()
        : Stream.of("Expected string, but got " + input.toPrettyString());
  }

  @Override
  public Stream<String> tuple(Stream<BasicType> contents) {
    if (input.isArray()) {
      return contents.flatMap(
          new Function<BasicType, Stream<String>>() {
            private int index;

            @Override
            public Stream<String> apply(BasicType inputType) {
              return inputType.apply(new CheckEngineType(input.get(index++)));
            }
          });
    } else {
      return Stream.of("Expected tuple (in an array), but got " + input.toPrettyString());
    }
  }
}
