package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.ActiveOperation;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep;
import ca.on.oicr.gsi.vidarr.OperationStatefulStep.Child;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Start the input provisioning tasks
 */
final class PrepareInputProvisioning implements InputType.Visitor<JsonNode> {

  private static <State extends Record> TaskStarter<JsonMutation> launch(
      InputProvisionFormat format,
      WorkflowLanguage language,
      InputProvisioner<State> provisioner,
      List<JsonPath> mutation,
      String id,
      String filePath) {
    return TaskStarter.of(
        "i" + format.name(),
        wrapProvisionActionInternal(language, provisioner, provisioner.build())
            .launch(new InputProvisioningStateInternal(mutation, id, filePath)));
  }

  private static <State extends Record> TaskStarter<JsonMutation> launchExternal(
      InputProvisionFormat format,
      WorkflowLanguage language,
      InputProvisioner<State> provisioner,
      List<JsonPath> mutation,
      JsonNode metadata) {
    return TaskStarter.of(
        "e" + format.name(),
        wrapProvisionActionExternal(language, provisioner, provisioner.build())
            .launch(new InputProvisioningStateExternal(mutation, metadata)));
  }

  public static <State extends Record> TaskStarter<JsonMutation> recover(
      WorkflowLanguage language,
      ActiveOperation<?> operation,
      InputProvisioner<State> provisioner) {
    switch (operation.type().charAt(0)) {
      case 'i':
        return TaskStarter.of(
            operation.type(),
            wrapProvisionActionInternal(language, provisioner, provisioner.build())
                .recover(operation.recoveryState()));
      case 'e':
        return TaskStarter.of(
            operation.type(),
            wrapProvisionActionExternal(language, provisioner, provisioner.build())
                .recover(operation.recoveryState()));
      default:
        throw new IllegalArgumentException("Illegal type for operation: " + operation.type());
    }
  }

  static <State extends Record, OriginalState extends Record>
  OperationAction<
      Child<InputProvisioningStateExternal, State>,
      InputProvisioningStateExternal,
      JsonMutation>
  wrapProvisionActionExternal(
      WorkflowLanguage language,
      InputProvisioner<OriginalState> provisioner,
      OperationAction<State, OriginalState, JsonNode> action) {
    return OperationAction.load(
            InputProvisioningStateExternal.class, InputProvisioningStateExternal::metadata)
        .then(
            OperationStatefulStep.subStep(
                (state, metadata) -> provisioner.prepareExternalProvisionInput(language, metadata),
                action))
        .map((state, result) -> new JsonMutation(state.state().mutation(), result));
  }

  static <State extends Record, OriginalState extends Record>
  OperationAction<
      Child<InputProvisioningStateInternal, State>,
      InputProvisioningStateInternal,
      JsonMutation>
  wrapProvisionActionInternal(
      WorkflowLanguage language,
      InputProvisioner<OriginalState> provisioner,
      OperationAction<State, OriginalState, JsonNode> action) {
    return OperationAction.load(
            InputProvisioningStateInternal.class, InputProvisioningStateInternal::id)
        .then(
            OperationStatefulStep.subStep(
                (state, id) -> provisioner.prepareInternalProvisionInput(language, id,
                    state.path()), action))
        .map((state, result) -> new JsonMutation(state.state().mutation(), result));
  }

  private final Consumer<TaskStarter<JsonMutation>> consumer;
  private final JsonNode input;
  private final List<JsonPath> jsonPath;
  private final WorkflowLanguage language;
  private final FileResolver resolver;
  private final Map<Integer, List<Consumer<ObjectNode>>> retryModifications;
  private final Target target;

  public PrepareInputProvisioning(
      Target target,
      JsonNode input,
      Stream<JsonPath> jsonPath,
      WorkflowLanguage language,
      FileResolver resolver,
      Consumer<TaskStarter<JsonMutation>> consumer,
      Map<Integer, List<Consumer<ObjectNode>>> retryModifications) {
    this.target = target;
    this.input = input;
    this.jsonPath = jsonPath.collect(Collectors.toList());
    this.language = language;
    this.resolver = resolver;
    this.consumer = consumer;
    this.retryModifications = retryModifications;
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
                    language,
                    resolver,
                    consumer,
                    retryModifications)));
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
        outputEntry.add(
            key.apply(
                new PrepareInputProvisioning(
                    target,
                    inputEntry.get(0),
                    Stream.concat(
                        jsonPath.stream(), Stream.of(JsonPath.array(i), JsonPath.array(0))),
                    language,
                    resolver,
                    consumer,
                    retryModifications)));
        outputEntry.add(
            key.apply(
                new PrepareInputProvisioning(
                    target,
                    inputEntry.get(1),
                    Stream.concat(
                        jsonPath.stream(), Stream.of(JsonPath.array(i), JsonPath.array(1))),
                    language,
                    resolver,
                    consumer,
                    retryModifications)));
        output.add(outputEntry);
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
              launchExternal(
                  format, language, handler, jsonPath, input.get("contents").get("configuration")));
          break;
        case "INTERNAL":
          if (input.has("contents")
              && input.get("contents").isArray()
              && input.get("contents").size() == 1
              && input.get("contents").get(0).isTextual()) {
            final var id = input.get("contents").get(0).asText();
            final var filePath = resolver.pathForId(id).map(FileMetadata::path).orElseThrow(
                () -> new IllegalArgumentException(
                    String.format("Could not resolve input file %s", id)));
            consumer.accept(launch(format, language, handler, jsonPath, id, filePath));
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
        output.add(
            inner.apply(
                new PrepareInputProvisioning(
                    target,
                    input.get(i),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.array(i))),
                    language,
                    resolver,
                    consumer,
                    retryModifications)));
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
                              language,
                              resolver,
                              consumer,
                              retryModifications))));
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
                    language,
                    resolver,
                    consumer,
                    retryModifications)));
        output.set(
            "right",
            right.apply(
                new PrepareInputProvisioning(
                    target,
                    input.get(1),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.object("right"))),
                    language,
                    resolver,
                    consumer,
                    retryModifications)));
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
                    language,
                    resolver,
                    consumer,
                    retryModifications)));
        output.set(
            "right",
            right.apply(
                new PrepareInputProvisioning(
                    target,
                    input.get("right"),
                    Stream.concat(jsonPath.stream(), Stream.of(JsonPath.object("right"))),
                    language,
                    resolver,
                    consumer,
                    retryModifications)));
        return output;

      } else {
        throw new IllegalArgumentException("Pair with incorrect fields");
      }

    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public JsonNode retry(BasicType inner) {
    final var fields = input.fields();
    while (fields.hasNext()) {
      final var field = fields.next();
      retryModifications
          .get(Integer.parseUnsignedInt(field.getKey()))
          .add(new ApplyRetry(jsonPath, field.getValue()));
    }
    return NullNode.getInstance();
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
                              language,
                              resolver,
                              consumer,
                              retryModifications)))
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
              output.add(
                  t.apply(
                      new PrepareInputProvisioning(
                          target,
                          input.get(index),
                          Stream.concat(jsonPath.stream(), Stream.of(JsonPath.array(index))),
                          language,
                          resolver,
                          consumer,
                          retryModifications)));
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
