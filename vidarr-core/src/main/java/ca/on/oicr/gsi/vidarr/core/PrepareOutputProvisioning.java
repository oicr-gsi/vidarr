package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisioner.Result;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Take the output metadata provided by the caller, the output from the workflow, and create tasks
 * for provisioning out the data
 */
final class PrepareOutputProvisioning
    extends BaseOutputExtractor<
        Stream<TaskStarter<Pair<ProvisionData, Result>>>,
        Stream<TaskStarter<Pair<ProvisionData, Result>>>> {
  static final class ProvisioningOutWorkMonitor
      extends WrappedMonitor<
          ProvisionData, OutputProvisioner.Result, Pair<ProvisionData, OutputProvisioner.Result>> {

    public ProvisioningOutWorkMonitor(
        ProvisionData accessory, WorkMonitor<Pair<ProvisionData, Result>, JsonNode> monitor) {
      super(accessory, monitor);
    }

    @Override
    protected Pair<ProvisionData, Result> mix(ProvisionData accessory, Result result) {
      return new Pair<>(accessory, result);
    }
  }

  private static Map<String, String> extractLabels(JsonNode node) {
    final var labels = new TreeMap<String, String>();

    final var iterator = node.fields();
    while (iterator.hasNext()) {
      final var entry = iterator.next();
      labels.put(entry.getKey(), entry.getValue().asText());
    }
    return labels;
  }

  private final Set<? extends ExternalId> allInputIds;
  private final ObjectMapper mapper;
  private final Set<? extends ExternalId> remainingInputIds;
  private final Runnable outputIsBad;
  private final Target target;

  public PrepareOutputProvisioning(
      ObjectMapper mapper,
      Target target,
      JsonNode output,
      JsonNode metadata,
      Set<? extends ExternalId> allInputIds,
      Set<? extends ExternalId> remainingInputIds,
      Runnable outputIsBad) {
    super(output, metadata);
    this.mapper = mapper;
    this.target = target;
    this.allInputIds = allInputIds;
    this.remainingInputIds = remainingInputIds;
    this.outputIsBad = outputIsBad;
  }

  @Override
  protected Stream<TaskStarter<Pair<ProvisionData, Result>>> handle(
      WorkflowOutputDataType format,
      boolean optional,
      JsonNode metadata,
      JsonNode outputs,
      OutputData outputData) {
    final var handler = Objects.requireNonNull(target.provisionerFor(format.format()));
    if (optional && output.isNull()) {
      return Stream.empty();
    }

    return (switch (format) {
          case DATAWAREHOUSE_RECORDS, LOGS, FILE, QUALITY_CONTROL -> Stream.of(
              new Pair<>(output.asText(), Map.<String, String>of()));
          case FILES -> stream(output, optional)
              .map(file -> new Pair<>(file.asText(), Map.<String, String>of()));

          case FILE_WITH_LABELS -> {
            final var labels = extractLabels(output.get("right"));
            yield Stream.of(new Pair<>(output.get("left").asText(), labels));
          }
          case FILES_WITH_LABELS -> {
            final var labels = extractLabels(output.get("right"));
            yield stream(output.get("left"), optional)
                .map(file -> new Pair<>(file.asText(), labels));
          }
        })
        .map(
            p -> {
              final var provisionData = new ProvisionData();
              provisionData.setLabels(p.second());
              provisionData.setIds(
                  outputData.visit(
                      new OutputDataVisitor<>() {
                        @Override
                        public Set<? extends ExternalId> all() {
                          return allInputIds;
                        }

                        @Override
                        public Set<? extends ExternalId> external(Stream<ExternalId> ids) {
                          return ids.collect(Collectors.toSet());
                        }

                        @Override
                        public Set<? extends ExternalId> remaining() {
                          return remainingInputIds;
                        }
                      }));
              return WrappedMonitor.start(
                  provisionData,
                  ProvisioningOutWorkMonitor::new,
                  (workflowLanguage, workflowId, o) ->
                      new Pair<>(
                          format.format().name(),
                          handler.provision(workflowId, p.first(), metadata, o)));
            });
  }

  @Override
  protected ObjectMapper mapper() {
    return mapper;
  }

  @Override
  protected Stream<TaskStarter<Pair<ProvisionData, Result>>> mergeChildren(
      Stream<Stream<TaskStarter<Pair<ProvisionData, Result>>>> stream) {
    return stream.flatMap(Function.identity());
  }

  @Override
  protected Stream<TaskStarter<Pair<ProvisionData, Result>>> processChild(
      Map<String, Object> key, String name, OutputType type, JsonNode metadata, JsonNode output) {
    return type.apply(
        new PrepareOutputProvisioning(
            mapper, target, output, metadata, allInputIds, remainingInputIds, outputIsBad));
  }

  private Stream<JsonNode> stream(JsonNode node, boolean optional) {
    if (!optional && node.isEmpty()) {
      outputIsBad.run();
    }
    return StreamSupport.stream(Spliterators.spliterator(node.iterator(), node.size(), 0), false);
  }

  @Override
  public Stream<TaskStarter<Pair<ProvisionData, Result>>> unknown() {
    throw new IllegalArgumentException();
  }
}
