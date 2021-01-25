package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Create all preflight check tasks from the metadata the caller provided */
final class PreparePreflightChecks extends BaseOutputExtractor<Boolean, Boolean> {

  private final Runnable extraInputIdsHandled;
  private final ObjectMapper mapper;
  private final Consumer<TaskStarter<Boolean>> preflightTask;
  private final Consumer<ExternalId> requestedExternalId;
  private final Target target;

  public PreparePreflightChecks(
      ObjectMapper mapper,
      Target target,
      JsonNode metadata,
      Runnable extraIdsHandled,
      Consumer<ExternalId> requestedExternalId,
      Consumer<TaskStarter<Boolean>> preflightTask) {
    super(null, metadata);
    this.mapper = mapper;
    this.target = target;
    this.extraInputIdsHandled = extraIdsHandled;
    this.requestedExternalId = requestedExternalId;
    this.preflightTask = preflightTask;
  }

  @Override
  protected Boolean handle(
      WorkflowOutputDataType format, JsonNode metadata, JsonNode output, OutputData outputData) {
    final var handler = target.provisionerFor(format.format());
    if (handler == null) {
      return false;
    } else {
      outputData.visit(
          new OutputDataVisitor<Void>() {
            @Override
            public Void all() {
              return null;
            }

            @Override
            public Void external(Stream<ExternalId> ids) {
              ids.forEach(requestedExternalId);
              return null;
            }

            @Override
            public Void remaining() {
              extraInputIdsHandled.run();
              return null;
            }
          });
      preflightTask.accept(
          (workflowLanguage, workflowId, o) ->
              new Pair<>(format.name(), handler.preflightCheck(metadata, o)));
      return true;
    }
  }

  @Override
  protected Boolean invalid() {
    return false;
  }

  @Override
  protected ObjectMapper mapper() {
    return mapper;
  }

  @Override
  protected Boolean mergeChildren(Stream<Boolean> stream) {
    return stream.allMatch(x -> x);
  }

  @Override
  protected Boolean processChild(
      Map<String, Object> key, OutputType type, JsonNode metadata, JsonNode output) {
    return type.apply(
        new PreparePreflightChecks(
            mapper, target, metadata, extraInputIdsHandled, requestedExternalId, preflightTask));
  }

  @Override
  public Boolean unknown() {
    return false;
  }
}
