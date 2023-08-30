package ca.on.oicr.gsi.vidarr.core;

import static ca.on.oicr.gsi.vidarr.core.BaseInputExtractor.EXTERNAL__CONFIG;
import static ca.on.oicr.gsi.vidarr.core.BaseInputExtractor.EXTERNAL__IDS;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Check that the input provided by the caller matches the workflow definition
 *
 * <p>The result will be a stream of errors; if empty, no errors were found.
 */
public final class CheckInputType implements InputType.Visitor<Stream<String>> {

  private final String context;
  private final JsonNode input;
  private final ObjectMapper mapper;
  private final Target target;

  public CheckInputType(ObjectMapper mapper, Target target, String context, JsonNode input) {
    this.mapper = mapper;
    this.target = target;
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
  public Stream<String> dictionary(InputType key, InputType value) {
    if (input.isObject() && key == InputType.STRING) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(v -> value.apply(new CheckInputType(mapper, target, context + "{value}", v)));
    }
    if (input.isArray()) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(
              e ->
                  e.isArray() && e.size() == 2
                      ? Stream.of(
                          String.format(
                              "%s: Expected inner key as array with two elements, but got %s",
                              context, e.toPrettyString()))
                      : Stream.concat(
                          key.apply(
                              new CheckInputType(mapper, target, context + "{key}", e.get(0))),
                          value.apply(
                              new CheckInputType(mapper, target, context + "{value}", e.get(1)))));
    } else {
      return Stream.of(
          String.format("%s: Expected dictionary, but got %s.", context, input.toPrettyString()));
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
            return Stream.of(String.format("%s: Input for external is not an object.", context));
          }
          if (!contents.has(EXTERNAL__IDS)) {
            return Stream.of(String.format("%s: Input for external is missing IDs.", context));
          }
          if (!contents.has(EXTERNAL__CONFIG)) {
            return Stream.of(
                String.format("%s: Input for external is missing configuration.", context));
          }
          final var externalIds = contents.get(EXTERNAL__IDS);
          if (!externalIds.isArray()) {
            return Stream.of(String.format("%s: External IDs are not an array.", context));
          }
          try {
            for (var id : mapper.treeToValue(externalIds, ExternalId[].class)) {
              if (id.getId() == null
                  || id.getId().isBlank()
                  || id.getProvider() == null
                  || id.getProvider().isBlank()) {
                return Stream.of(
                    String.format(
                        "%s: External IDs has blank or missing ID or provider.", context));
              }
            }
          } catch (JsonProcessingException e) {
            return Stream.of(
                String.format("%s: External IDs are malformed: %s", context, e.getMessage()));
          }
          return target
              .provisionerFor(format)
              .externalTypeFor(format)
              .apply(new ValidateJsonToSimpleType(context, contents.get(EXTERNAL__CONFIG)));
        case "INTERNAL":
          if (contents.isArray() && contents.size() == 1 && contents.get(0).isTextual()) {
            final var id = contents.get(0).asText();
            final var matcher = BaseProcessor.ANALYSIS_RECORD_ID.matcher(id);
            if (matcher.matches()) {
              if (matcher.group("type").equals("file")) {
                return Stream.empty();
              } else {
                return Stream.of(
                    String.format("%s: Analysis record ID “%s” is not a file", context, id));
              }
            } else {
              return Stream.of(
                  String.format("%s: Analysis record ID “%s” is malformed", context, id));
            }
          } else {
            return Stream.of(
                String.format("%s: Reference to internal data lacks a single ID.", context));
          }
        default:
          return Stream.of(
              String.format(
                  "%s: Unknown type %s in tagged union for %s.",
                  context, input.get("type").asText(), format));
      }
    } else {
      return Stream.of(
          String.format(
              "%s: Expected tagged union of INTERNAL or EXTERNAL for %s but got %s.",
              context, format, input.toPrettyString()));
    }
  }

  @Override
  public Stream<String> file() {
    return handle(InputProvisionFormat.FILE);
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
  public Stream<String> list(InputType inner) {
    if (input.isArray()) {
      return StreamSupport.stream(input.spliterator(), false)
          .flatMap(
              new Function<JsonNode, Stream<? extends String>>() {
                private int index;

                @Override
                public Stream<? extends String> apply(JsonNode e) {
                  return inner.apply(
                      new CheckInputType(mapper, target, context + "[" + (index++) + "]", e));
                }
              });
    } else {
      return Stream.of(context + ": Expected list (as array), but got " + input.toPrettyString());
    }
  }

  @Override
  public Stream<String> object(Stream<Pair<String, InputType>> contents) {
    if (input.isObject()) {
      return contents.flatMap(
          p ->
              input.has(p.first())
                  ? p.second()
                      .apply(
                          new CheckInputType(
                              mapper, target, context + "." + p.first(), input.get(p.first())))
                  : Stream.of(
                      String.format("%s: Missing attribute %s in object.", context, p.first())));
    } else {
      return Stream.of(
          String.format("%s: Expected object, but got %s.", context, input.getNodeType().name()));
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
          left.apply(new CheckInputType(mapper, target, context + ".left", input.get("left"))),
          right.apply(new CheckInputType(mapper, target, context + ".right", input.get("right"))));
    } else {
      return Stream.of(
          String.format(
              "%s: Expected pair (as object with left and right), but got %s.",
              context, input.toPrettyString()));
    }
  }

  @Override
  public Stream<String> retry(BasicType inner) {
    if (input.isObject()) {
      return Stream.concat(
          StreamSupport.stream(Spliterators.spliterator(input.fields(), input.size(), 0), false)
              .flatMap(
                  e ->
                      Stream.concat(
                          e.getKey().chars().allMatch(Character::isDigit)
                              ? Stream.empty()
                              : Stream.of(context + "[" + e.getKey() + "]: Key is not numeric"),
                          inner.apply(
                              new ValidateJsonToSimpleType(
                                  context + "[" + e.getKey() + "]", e.getValue())))),
          input.has("0") ? Stream.empty() : Stream.of(context + ": No entry for 0 is present."));
    }
    return Stream.of(context + ": Expected object, but got " + input.toPrettyString());
  }

  @Override
  public Stream<String> string() {
    return input.isTextual()
        ? Stream.empty()
        : Stream.of(context + ": Expected string, but got " + input.toPrettyString());
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
            .map(
                e ->
                    e.getValue()
                        .apply(
                            new CheckInputType(
                                mapper, target, context + " " + e.getKey(), input.get("contents"))))
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
  public Stream<String> tuple(Stream<InputType> contents) {
    if (input.isArray()) {
      return contents.flatMap(
          new Function<InputType, Stream<String>>() {
            private int index;

            @Override
            public Stream<String> apply(InputType inputType) {
              return inputType.apply(
                  new CheckInputType(
                      mapper, target, context + "[" + index + "]", input.get(index++)));
            }
          });
    } else {
      return Stream.of(
          String.format(
              "%s: Expected tuple (in an array), but got %s.", context, input.toPrettyString()));
    }
  }
}
