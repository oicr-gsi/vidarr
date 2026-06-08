package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.core.*;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/** A processor designed to run a single workflow run */
final class SingleShotProcessor
    extends BaseProcessor<SingleShotWorkflow, SingleShotOperation, Void> {
  public enum Status {
    BAD_ARGUMENTS,
    FAILURE,
    SUCCESS
  }

  private static final JsonMapper MAPPER =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .build();

  protected SingleShotProcessor(ScheduledExecutorService executor) {
    super(executor);
  }

  @Override
  protected JsonMapper mapper() {
    return MAPPER;
  }

  @Override
  public Optional<FileMetadata> pathForId(String id) {
    return Optional.empty();
  }

  public Status run(
      String prefix,
      Target target,
      WorkflowDefinition workflow,
      ObjectNode arguments,
      ObjectNode metadata,
      JsonNode engineParameters,
      OutputProvisioningHandler<Void> outputHandler) {
    final SingleShotWorkflow active =
        startAsync(prefix, target, workflow, arguments, metadata, engineParameters, outputHandler);
    if (active == null) return Status.BAD_ARGUMENTS;
    return active.await() ? Status.SUCCESS : Status.FAILURE;
  }

  public SingleShotWorkflow startAsync(
      String prefix,
      Target target,
      WorkflowDefinition workflow,
      ObjectNode arguments,
      ObjectNode metadata,
      JsonNode engineParameters,
      OutputProvisioningHandler<Void> outputHandler) {
    System.err.printf("%s: [%s] Validating input...%n", prefix, Instant.now());
    final List<String> errors =
        validateInput(MAPPER, target, workflow, arguments, metadata, engineParameters)
            .collect(Collectors.toList());
    if (!errors.isEmpty()) {
      errors.forEach(System.err::println);
      return null;
    }
    System.err.printf("%s: [%s] Starting workflow...%n", prefix, Instant.now());
    final var active =
        new SingleShotWorkflow(
            prefix,
            arguments,
            engineParameters,
            metadata,
            extractInputVidarrIds(mapper(), workflow, arguments)
                .flatMap(
                    i ->
                        this.pathForId(i).map(FileMetadata::externalKeys).orElseGet(Stream::empty)),
            outputHandler);
    start(target, workflow, active, null);
    System.err.printf("%s: [%s] Waiting for completion...%n", prefix, Instant.now());
    return active;
  }

  @Override
  public void inTransaction(Consumer<Void> operation) {
    operation.accept(null);
  }
}
