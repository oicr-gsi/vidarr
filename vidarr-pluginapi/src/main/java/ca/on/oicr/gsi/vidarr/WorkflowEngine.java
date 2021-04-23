package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

/** Defines an engine that knows how to execute workflows and track the results */
@JsonTypeIdResolver(WorkflowEngine.WorkflowEngineIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface WorkflowEngine {
  final class WorkflowEngineIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends WorkflowEngine>> knownIds =
        ServiceLoader.load(WorkflowEngineProvider.class).stream()
            .map(Provider::get)
            .flatMap(WorkflowEngineProvider::types)
            .collect(Collectors.toMap(Pair::first, Pair::second));

    @Override
    public Id getMechanism() {
      return Id.CUSTOM;
    }

    @Override
    public String idFromValue(Object o) {
      return knownIds.entrySet().stream()
          .filter(known -> known.getValue().isInstance(o))
          .map(Entry::getKey)
          .findFirst()
          .orElseThrow();
    }

    @Override
    public String idFromValueAndType(Object o, Class<?> aClass) {
      return idFromValue(o);
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) throws IOException {
      final var clazz = knownIds.get(id);
      return clazz == null ? null : context.constructType(clazz);
    }
  }

  /** The output data from a workflow */
  class Result<C> {
    private final Optional<C> cleanupState;
    private final JsonNode output;
    private final String workflowRunUrl;

    /**
     * Create new workflow output data
     *
     * @param output the JSON data emitted by the workflow
     * @param workflowRunUrl the URL of the completed workflow run for provisioning metrics and logs
     * @param cleanupState a state that can be used later to trigger cleanup of the workflow's
     *     output once provisioning out has been completed
     */
    public Result(JsonNode output, String workflowRunUrl, Optional<C> cleanupState) {
      this.output = output;
      this.workflowRunUrl = workflowRunUrl;
      this.cleanupState = cleanupState;
    }

    /** The information the plugin requires to clean up the output, if required. */
    public Optional<C> cleanupState() {
      return cleanupState;
    }

    /** The workflow output's output */
    public JsonNode output() {
      return output;
    }

    /**
     * The URL identifying the workflow run so the metrics and logs workflows can collect
     * information about the completed job.
     */
    public String workflowRunUrl() {
      return workflowRunUrl;
    }
  }

  /**
   * Clean up the output of a workflow (i.e., delete its on-disk output) after provisioning has been
   * completed.
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link WorkMonitor#scheduleTask(Runnable)} callback so that Vidarr can execute it once the
   * database is in a healthy state.
   *
   * @param cleanupState the clean up state provided with the workflow's output
   * @param monitor the monitor structure for clean up process; since no output is required, supply
   *     null as the output value
   */
  JsonNode cleanup(JsonNode cleanupState, WorkMonitor<Void, JsonNode> monitor);

  /** Display configuration status */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /**
   * Parameters that the submitter can provide to control the behaviour of the workflow engine.
   *
   * <p>Vidarr is conceptually neutral as to what goes here. This is provided as a side channel to
   * get configuration parameters to the backend.
   */
  Optional<BasicType> engineParameters();

  /**
   * Restart a running process from state saved in the database
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link WorkMonitor#scheduleTask(Runnable)} callback so that Vidarr can execute it once the
   * database is in a healthy state.
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the workflow process
   */
  void recover(JsonNode state, WorkMonitor<Result<JsonNode>, JsonNode> monitor);

  /**
   * Restart a running clean up process from state saved in the database
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link WorkMonitor#scheduleTask(Runnable)} callback so that Vidarr can execute it once the
   * database is in a healthy state.
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the cleanup process
   */
  void recoverCleanup(JsonNode state, WorkMonitor<Void, JsonNode> monitor);

  /**
   * Start a new workflow
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link WorkMonitor#scheduleTask(Runnable)} callback so that Vidarr can execute it once the
   * database is in a healthy state.
   *
   * @param workflowLanguage the language the workflow was written in
   * @param workflow the contents of the workflow
   * @param accessoryFiles additional files that should be included with the workflow
   * @param vidarrId the ID of the workflow being executed
   * @param workflowParameters the input parameters to the workflow
   * @param engineParameters the input configuration parameters to the workflow engine
   * @param monitor the monitor structure for writing the output of the workflow process
   * @return the initial state of the provision out process
   */
  JsonNode run(
      WorkflowLanguage workflowLanguage,
      String workflow,
      Stream<Pair<String, String>> accessoryFiles,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters,
      WorkMonitor<Result<JsonNode>, JsonNode> monitor);

  /**
   * Called to initialise this workflow engine.
   *
   * <p>If the configuration is invalid, this should throw a runtime exception.
   */
  void startup();

  /**
   * Checks if this engine can support this language
   *
   * @param language the workflow language to check
   */
  boolean supports(WorkflowLanguage language);
}
