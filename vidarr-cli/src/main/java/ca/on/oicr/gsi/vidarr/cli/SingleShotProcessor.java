package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A processor designed to run a single workflow run */
final class SingleShotProcessor
    extends BaseProcessor<SingleShotWorkflow, SingleShotOperation, SingleShotTransaction> {
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
      OutputProvisioningHandler<SingleShotTransaction> outputHandler) {
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
      OutputProvisioningHandler<SingleShotTransaction> outputHandler) {
    System.err.printf("%s: [%s] Validating input...%n", prefix, Instant.now());
    final List<String> errors =
        validateInput(target, workflow, arguments, metadata, engineParameters)
            .collect(Collectors.toList());
    if (!errors.isEmpty()) {
      errors.forEach(System.err::println);
      return null;
    }
    System.err.printf("%s: [%s] Starting workflow...%n", prefix, Instant.now());
    final var active =
        new SingleShotWorkflow(prefix, arguments, engineParameters, metadata, outputHandler);
    start(target, workflow, active, startTransaction());
    System.err.printf("%s: [%s] Waiting for completion...%n", prefix, Instant.now());
    return active;
  }

  public Stream<String> validateInput(
      Target target,
      WorkflowDefinition workflow,
      ObjectNode arguments,
      ObjectNode metadata,
      ObjectNode engineParameters) {
    return Stream.of(
            workflow
                .outputs()
                .flatMap(
                    o ->
                        metadata.has(o.name())
                            ? o.type()
                                .apply(new CheckOutputType(MAPPER, target, metadata.get(o.name())))
                            : Stream.of("Missing metadata attribute " + o.name())),
            workflow
                .parameters()
                .flatMap(
                    p -> {
                      if (arguments.has(p.name())) {
                        return p.type()
                            .apply(new CheckInputType(MAPPER, target, arguments.get(p.name())));
                      } else {
                        return p.isRequired()
                            ? Stream.of("Required argument missing: " + p.name())
                            : Stream.empty();
                      }
                    }),
            target
                .engine()
                .engineParameters()
                .map(p -> p.apply(new CheckEngineType(engineParameters)))
                .orElseGet(Stream::empty))
        .flatMap(Function.identity());
  }

  @Override
  protected SingleShotTransaction startTransaction() {
    return new SingleShotTransaction();
  }
}
