package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Start the input provisioning tasks */
final class PrepareInputProvisioning implements InputType.Visitor<JsonNode> {

  public static class ProvisionInMonitor
      extends WrappedMonitor<List<JsonPath>, JsonNode, JsonMutation> {

    public ProvisionInMonitor(
        List<JsonPath> jsonPath, WorkMonitor<JsonMutation, JsonNode> monitor) {
      super(jsonPath, monitor);
    }

    @Override
    protected JsonMutation mix(List<JsonPath> accessory, JsonNode result) {
      return new JsonMutation(result, accessory.stream());
    }
  }

  private final Consumer<TaskStarter<JsonMutation>> consumer;
  private final JsonNode input;
  private final List<JsonPath> jsonPath;
  private final FileResolver resolver;
  private final Target target;

  public PrepareInputProvisioning(
      Target target,
      JsonNode input,
      Stream<JsonPath> jsonPath,
      FileResolver resolver,
      Consumer<TaskStarter<JsonMutation>> consumer) {
    this.target = target;
    this.input = input;
    this.jsonPath = jsonPath.collect(Collectors.toList());
    this.resolver = resolver;
    this.consumer = consumer;
  }

  @Override
  public JsonNode bool() {
    if (input.isBoolean()) {
      return input;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode date() {
    if (input.isIntegralNumber() || input.isTextual()) {
      return input;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode dictionary(InputType key, InputType value) {
    if (input.isObject() && key == InputType.STRING) {
      final var output = JsonNodeFactory.instance.objectNode();
      final var iterator = input.fields();
      while (iterator.hasNext()) {
        final var entry = iterator.next();
        output.set(
            entry.getKey(),
            value.apply(
                new PrepareInputProvisioning(
                    target,
                    entry.getValue(),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.object(entry.getKey()))),
                    resolver,
                    consumer)));
      }
      return output;
    } else if (input.isArray()) {
      final var output = JsonNodeFactory.instance.arrayNode(input.size());
      for (var i = 0; i < input.size(); i++) {
        final var outputEntry = JsonNodeFactory.instance.arrayNode(2);
        final var inputEntry = input.get(i);
        if (!inputEntry.isArray() || inputEntry.size() != 2) {
          throw new IllegalArgumentException();
        }
        outputEntry.set(
            0,
            key.apply(
                new PrepareInputProvisioning(
                    target,
                    inputEntry.get(0),
                    Stream.concat(
                        jsonPath.stream(), Stream.of(JsonPath.array(i), JsonPath.array(0))),
                    resolver,
                    consumer)));
        outputEntry.set(
            1,
            key.apply(
                new PrepareInputProvisioning(
                    target,
                    inputEntry.get(1),
                    Stream.concat(
                        jsonPath.stream(), Stream.of(JsonPath.array(i), JsonPath.array(1))),
                    resolver,
                    consumer)));
        output.set(i, outputEntry);
      }
      return output;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode directory() {
    return handle(InputProvisionFormat.DIRECTORY);
  }

  @Override
  public JsonNode file() {
    return handle(InputProvisionFormat.FILE);
  }

  @Override
  public JsonNode floating() {
    if (input.isNumber()) {
      return input;
    } else {
      throw new IllegalArgumentException();
    }
  }

  private JsonNode handle(InputProvisionFormat format) {
    final var handler = target.provisionerFor(format);
    if (handler == null) {
      throw new UnsupportedOperationException("No handler for " + format.name());
    }
    if (input.isObject() && input.has("type") && input.get("type").isTextual()) {
      switch (input.get("type").asText()) {
        case "EXTERNAL":
          consumer.accept(
              (language, workflowId, monitor) ->
                  new Pair<>(
                      format.name(),
                      handler.provisionExternal(
                          language,
                          input.get("contents").get("configuration"),
                          new ProvisionInMonitor(jsonPath, monitor))));
          break;
        case "INTERNAL":
          if (input.has("contents")
              && input.get("contents").isArray()
              && input.get("contents").size() == 1
              && input.get("contents").get(0).isTextual()) {
            final var id = input.get("contents").get(0).asText();
            final var filePath = resolver.pathForId(id).map(FileMetadata::path).orElseThrow();
            consumer.accept(
                (language, workflowId, monitor) ->
                    new Pair<>(
                        format.name(),
                        handler.provision(
                            language, id, filePath, new ProvisionInMonitor(jsonPath, monitor))));
          } else {
            throw new IllegalArgumentException("Invalid input file for BY_ID");
          }
          break;
      }
    }

    return NullNode.getInstance();
  }

  @Override
  public JsonNode integer() {
    if (input.isIntegralNumber()) {
      return input;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode json() {
    return input;
  }

  @Override
  public JsonNode list(InputType inner) {
    if (input.isArray()) {
      final var output = JsonNodeFactory.instance.arrayNode(input.size());
      for (var i = 0; i < input.size(); i++) {
        output.set(
            i,
            inner.apply(
                new PrepareInputProvisioning(
                    target,
                    input.get(i),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.array(i))),
                    resolver,
                    consumer)));
      }
      return output;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode object(Stream<Pair<String, InputType>> contents) {
    if (input.isObject()) {
      final var output = JsonNodeFactory.instance.objectNode();
      contents.forEach(
          p ->
              output.set(
                  p.first(),
                  p.second()
                      .apply(
                          new PrepareInputProvisioning(
                              target,
                              input.get(p.first()),
                              Stream.concat(
                                  jsonPath.stream(), Stream.of(JsonPath.object(p.first()))),
                              resolver,
                              consumer))));
      return output;
    } else {
      contents.close();
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode optional(InputType inner) {
    if (input.isNull()) {
      return input;
    } else {
      return inner.apply(this);
    }
  }

  @Override
  public JsonNode pair(InputType left, InputType right) {
    if (input.isArray()) {
      if (input.size() == 2) {
        final var output = JsonNodeFactory.instance.objectNode();
        output.set(
            "left",
            left.apply(
                new PrepareInputProvisioning(
                    target,
                    input.get(0),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.object("left"))),
                    resolver,
                    consumer)));
        output.set(
            "right",
            right.apply(
                new PrepareInputProvisioning(
                    target,
                    input.get(1),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.object("right"))),
                    resolver,
                    consumer)));
        return output;
      } else {
        throw new IllegalArgumentException("Pair with incorrect number of arguments");
      }
    } else if (input.isObject()) {
      if (input.has("left") && input.has("right")) {
        final var output = JsonNodeFactory.instance.objectNode();
        output.set(
            "left",
            left.apply(
                new PrepareInputProvisioning(
                    target,
                    input.get("left"),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.object("left"))),
                    resolver,
                    consumer)));
        output.set(
            "right",
            right.apply(
                new PrepareInputProvisioning(
                    target,
                    input.get("right"),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.object("right"))),
                    resolver,
                    consumer)));
        return output;

      } else {
        throw new IllegalArgumentException("Pair with incorrect fields");
      }

    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode string() {
    if (input.isTextual()) {
      return input;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode taggedUnion(Stream<Map.Entry<String, InputType>> elements) {
    if (input.isObject() && input.get("type").isTextual()) {
      final var type = input.get("type").asText();
      return elements
          .filter(e -> e.getKey().equals(type))
          .findFirst()
          .map(
              e ->
                  e.getValue()
                      .apply(
                          new PrepareInputProvisioning(
                              target,
                              input.get("contents"),
                              jsonPath.stream(),
                              resolver,
                              consumer)))
          .orElseThrow();
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode tuple(Stream<InputType> contents) {
    if (input.isArray()) {
      final var output = JsonNodeFactory.instance.arrayNode(input.size());
      contents.forEach(
          new Consumer<>() {
            private int index;

            @Override
            public void accept(InputType t) {
              output.set(
                  index,
                  t.apply(
                      new PrepareInputProvisioning(
                          target,
                          input.get(index),
                          Stream.concat(jsonPath.stream(), Stream.of(JsonPath.array(index))),
                          resolver,
                          consumer)));
              index++;
            }
          });
      return output;
    } else {
      contents.close();
      throw new IllegalArgumentException();
    }
  }
}
