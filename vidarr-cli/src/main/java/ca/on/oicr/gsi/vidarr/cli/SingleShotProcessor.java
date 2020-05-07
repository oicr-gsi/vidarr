package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
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
      Target target,
      WorkflowDefinition workflow,
      ObjectNode arguments,
      ObjectNode metadata,
      ObjectNode engineParameters,
      Supplier<ObjectNode> supplier) {
    System.err.println("Validating input...");
    final var errors =
        Stream.of(
                workflow
                    .outputs()
                    .flatMap(
                        o ->
                            metadata.has(o.name())
                                ? o.type()
                                    .apply(
                                        new CheckOutputType(MAPPER, target, metadata.get(o.name())))
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
                    .stream())
            .flatMap(Function.identity())
            .collect(Collectors.toList());
    if (!errors.isEmpty()) {
      errors.forEach(System.err::println);
      return Status.BAD_ARGUMENTS;
    }
    System.err.println("Starting workflow...");
    final var active = new SingleShotWorkflow(arguments, engineParameters, metadata, supplier);
    start(target, workflow, active, startTransaction());
    System.err.println("Waiting for completion...");
    active.await();
    return active.isSuccessful() ? Status.SUCCESS : Status.FAILURE;
  }

  @Override
  protected SingleShotTransaction startTransaction() {
    return new SingleShotTransaction();
  }
}
