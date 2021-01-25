package ca.on.oicr.gsi.vidarr.core;

import static ca.on.oicr.gsi.vidarr.core.BaseInputExtractor.EXTERNAL__CONFIG;
import static ca.on.oicr.gsi.vidarr.core.BaseInputExtractor.EXTERNAL__IDS;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Check that the input provided by the caller matches the workflow definition
 *
 * <p>The result will be a stream of errors; if empty, no errors were found.
 */
public final class CheckInputType implements InputType.Visitor<Stream<String>> {
  private final JsonNode input;
  private final ObjectMapper mapper;
  private final Target target;

  public CheckInputType(ObjectMapper mapper, Target target, JsonNode input) {
    this.mapper = mapper;
    this.target = target;
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
  public Stream<String> dictionary(InputType key, InputType value) {
    if (input.isObject() && key == InputType.STRING) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(v -> value.apply(new CheckInputType(mapper, target, v)));
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
                          key.apply(new CheckInputType(mapper, target, e.get(0))),
                          value.apply(new CheckInputType(mapper, target, e.get(1)))));
    } else {
      return Stream.of("Expected dictionary, but got " + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> directory() {
    return handle(InputProvisionFormat.DIRECTORY);
  }

  private Stream<String> handle(InputProvisionFormat format) {
    if (input.isObject()
        && input.has("contents")
        && input.has("type")
        && input.get("type").isTextual()) {
      final var contents = input.get("contents");
      switch (input.get("type").asText()) {
        case "EXTERNAL":
          if (!contents.isObject()) {
            return Stream.of("Input for external is not an object");
          }
          if (!contents.has(EXTERNAL__IDS)) {
            return Stream.of("Input for external is missing IDs");
          }
          if (!contents.has(EXTERNAL__CONFIG)) {
            return Stream.of("Input for external is missing configuration");
          }
          final var externalIds = contents.get(EXTERNAL__IDS);
          if (!externalIds.isArray()) {
            return Stream.of("External IDs are not an array");
          }
          try {
            for (var id : mapper.treeToValue(externalIds, ExternalId[].class)) {
              if (id.getId() == null
                  || id.getId().isBlank()
                  || id.getProvider() == null
                  || id.getProvider().isBlank()) {
                return Stream.of("External IDs has blank or missing ID or provider");
              }
            }
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          return target
              .provisionerFor(format)
              .externalTypeFor(format)
              .apply(new CheckEngineType(contents.get(EXTERNAL__CONFIG)));
        case "INTERNAL":
          return (contents.isArray() && contents.size() == 1 && contents.get(0).isTextual())
              ? Stream.empty()
              : Stream.of("Reference to internal data lacks a single ID");
        default:
          return Stream.of(
              "Unknown type " + input.get("type").asText() + " in tagged union for " + format);
      }
    } else {
      return Stream.of(
          "Expected tagged union of INTERNAL or EXTERNAL for "
              + format
              + " but got "
              + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> file() {
    return handle(InputProvisionFormat.FILE);
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
  public Stream<String> list(InputType inner) {
    if (input.isArray()) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(e -> inner.apply(new CheckInputType(mapper, target, e)));
    } else {
      return Stream.of("Expected list (as array), but got " + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> object(Stream<Pair<String, InputType>> contents) {
    if (input.isObject()) {
      return contents.flatMap(
          p ->
              input.has(p.first())
                  ? p.second().apply(new CheckInputType(mapper, target, input.get(p.first())))
                  : Stream.of("Missing attribute " + p.first() + " in object"));
    } else {
      return Stream.of("Expected object, but got " + input.getNodeType().name());
    }
  }

  @Override
  public Stream<String> optional(InputType inner) {
    return input.isNull() ? Stream.empty() : inner.apply(this);
  }

  @Override
  public Stream<String> pair(InputType left, InputType right) {
    if (input.isObject() && input.has("left") && input.has("right")) {
      return Stream.concat(
          left.apply(new CheckInputType(mapper, target, input.get("left"))),
          right.apply(new CheckInputType(mapper, target, input.get("right"))));
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
  public Stream<String> taggedUnion(Stream<Map.Entry<String, InputType>> elements) {
    if (input.isObject() && input.has("type") && input.has("contents")) {
      final var type = input.get("type");
      if (type.isTextual()) {
        final var typeStr = type.asText();
        return elements
            .filter(e -> e.getKey().equals(typeStr))
            .findAny()
            .map(e -> e.getValue().apply(new CheckInputType(mapper, target, input.get("contents"))))
            .orElseGet(() -> Stream.of("Unkown type in tagged union: " + type.asText()));
      } else {
        return Stream.of("Expected string type in tagged union, but got " + type.toPrettyString());
      }
    } else {
      return Stream.of("Expected tagged union (as object), but got " + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> tuple(Stream<InputType> contents) {
    if (input.isArray()) {
      return contents.flatMap(
          new Function<InputType, Stream<String>>() {
            private int index;

            @Override
            public Stream<String> apply(InputType inputType) {
              return inputType.apply(new CheckInputType(mapper, target, input.get(index++)));
            }
          });
    } else {
      return Stream.of("Expected tuple (in an array), but got " + input.toPrettyString());
    }
  }
}
