package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A processor designed to run a single workflow run */
final class SingleShotProcessor
    extends BaseProcessor<SingleShotWorkflow, SingleShotOperation, Void> {
  public enum Status {
    BAD_ARGUMENTS,
    FAILURE,
    SUCCESS
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected SingleShotProcessor(ScheduledExecutorService executor) {
    super(executor);
  }

  @Override
  protected ObjectMapper mapper() {
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
      ObjectNode engineParameters,
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
      ObjectNode engineParameters,
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
  protected void startTransaction(Consumer<Void> operation) {
    operation.accept(null);
  }
}
