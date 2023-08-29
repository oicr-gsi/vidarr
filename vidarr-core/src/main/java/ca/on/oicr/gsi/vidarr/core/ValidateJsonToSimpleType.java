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
 * Check the provided JSON matches a simple type
 *
 * <p>The result will be a stream of errors; if empty, no errors were found.
 */
public final class ValidateJsonToSimpleType implements BasicType.Visitor<Stream<String>> {

  private final String context;
  private final JsonNode input;

  public ValidateJsonToSimpleType(String context, JsonNode input) {
    this.context = context;
    this.input = input;
  }

  @Override
  public Stream<String> bool() {
    return input.isBoolean()
        ? Stream.empty()
        : Stream.of(
            String.format("%s: Expected Boolean but got %s.", context, input.toPrettyString()));
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
    return Stream.of(
        String.format("%s: Expected date but got %s.", context, input.toPrettyString()));
  }

  @Override
  public Stream<String> dictionary(BasicType key, BasicType value) {
    if (input.isObject() && key == BasicType.STRING) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(v -> value.apply(new ValidateJsonToSimpleType(context + "{value}", v)));
    }
    if (input.isArray()) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(
              e ->
                  e.isArray() && e.size() == 2
                      ? Stream.of(
                          String.format(
                              "%s: Expected inner key as array with two elements, but got %s.",
                              context, e.toPrettyString()))
                      : Stream.concat(
                          key.apply(new ValidateJsonToSimpleType(context + "{key}", e.get(0))),
                          value.apply(
                              new ValidateJsonToSimpleType(context + "{value}", e.get(1)))));
    } else {
      return Stream.of(
          String.format("%s: Expected dictionary, but got %s.", context, input.toPrettyString()));
    }
  }

  @Override
  public Stream<String> floating() {
    return input.isNumber()
        ? Stream.empty()
        : Stream.of(
            String.format("%s: Expected float but got %s.", context, input.toPrettyString()));
  }

  @Override
  public Stream<String> integer() {
    return input.isIntegralNumber()
        ? Stream.empty()
        : Stream.of(
            String.format("%s: Expected integer but got %s.", context, input.toPrettyString()));
  }

  @Override
  public Stream<String> json() {
    return Stream.empty();
  }

  @Override
  public Stream<String> list(BasicType inner) {
    if (input.isArray()) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(
              new Function<JsonNode, Stream<? extends String>>() {
                private int index;

                @Override
                public Stream<? extends String> apply(JsonNode e) {
                  return inner.apply(
                      new ValidateJsonToSimpleType(context + "[" + (index++) + "]", e));
                }
              });
    } else {
      return Stream.of(
          String.format(
              "%s: Expected list (as array), but got %s.", context, input.toPrettyString()));
    }
  }

  @Override
  public Stream<String> object(Stream<Pair<String, BasicType>> contents) {
    if (input.isObject()) {
      return contents.flatMap(
          p ->
              input.has(p.first())
                  ? p.second().apply(new ValidateJsonToSimpleType(context, input.get(p.first())))
                  : Stream.of(
                      String.format("%s: Missing attribute %s in object.", context, p.first())));
    } else {
      return Stream.of(
          String.format("%s: Expected object, but got %s.", context, input.getNodeType().name()));
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
            .map(
                e ->
                    e.getValue()
                        .apply(new ValidateJsonToSimpleType(context, input.get("contents"))))
            .orElseGet(
                () ->
                    Stream.of(
                        String.format(
                            "%s: Unknown type in tagged union: %s", context, type.asText())));
      } else {
        return Stream.of(
            String.format(
                "%s: Expected string type in tagged union, but got %s.",
                context, type.toPrettyString()));
      }
    } else {
      return Stream.of(
          String.format(
              "%s: Expected tagged union (as object), but got %s.",
              context, input.toPrettyString()));
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
          left.apply(new ValidateJsonToSimpleType(context + ".left", input.get("left"))),
          right.apply(new ValidateJsonToSimpleType(context + ".right", input.get("right"))));
    } else {
      return Stream.of(
          String.format(
              "%s: Expected pair (as object with left and right), but got %s.",
              context, input.toPrettyString()));
    }
  }

  @Override
  public Stream<String> string() {
    return input.isTextual()
        ? Stream.empty()
        : Stream.of(
            String.format("%s: Expected string, but got %s.", context, input.toPrettyString()));
  }

  @Override
  public Stream<String> tuple(Stream<BasicType> contents) {
    if (input.isArray()) {
      return contents.flatMap(
          new Function<BasicType, Stream<String>>() {
            private int index;

            @Override
            public Stream<String> apply(BasicType inputType) {
              return inputType.apply(
                  new ValidateJsonToSimpleType(context + "[" + index + "]", input.get(index++)));
            }
          });
    } else {
      return Stream.of(
          String.format(
              "%s: Expected tuple (in an array), but got %s.", context, input.toPrettyString()));
    }
  }
}
