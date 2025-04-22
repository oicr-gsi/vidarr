package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationAction.MAPPER;

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

/**
 * Defines an engine that knows how to execute workflows and track the results
 *
 * @param <State> the initial state information used to launch the workflow
 * @param <CleanupState> the state used to perform any cleanup after provision out
 */
@JsonTypeIdResolver(WorkflowEngine.WorkflowEngineIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface WorkflowEngine<State extends Record, CleanupState extends Record> {

  /**
   * The output data from a workflow
   *
   * @param output the JSON data emitted by the workflow
   * @param workflowRunUrl the URL of the completed workflow run for provisioning metrics and logs
   * @param cleanupState a state that can be used later to trigger cleanup of the workflow's output
   *     once provisioning out has been completed
   */
  record Result<C>(JsonNode output, String workflowRunUrl, Optional<C> cleanupState) {

    /**
     * Convert the cleanup state to a JSON object
     *
     * @return a replacement output with the cleanup state serialized, if present
     */
    public Result<JsonNode> serialize() {
      return new WorkflowEngine.Result<>(
          output(), workflowRunUrl(), cleanupState().map(MAPPER::valueToTree));
    }
  }

  final class WorkflowEngineIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends WorkflowEngine<?, ?>>> knownIds =
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

  /**
   * Create a declarative structure to clean up the output of a workflow
   *
   * <p>This method should not do any externally-visible work. It creates a set of operations so
   * that Vidarr can execute it once the database is in a healthy state.
   */
  OperationAction<?, CleanupState, Void> cleanup();

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
   * Build a declarative structure which executes a workflow when played back
   *
   * Compare to Pattern.compile() - builds a ready object for later use by the processor.
   * This allows the processor to play back and pause the sequence of events built here.
   *
   * @return the sequence of operations that should be performed
   */
  OperationAction<?, State, Result<CleanupState>> build();

  /**
   * Prepare the input to a new workflow
   *
   * <p>This method should not do any externally-visible work. It should simply populate the state
   * structure that will be used by the object created in {@link #build()}.
   * If build() is compared to Pattern.compile(), then prepareInput prepares the String that will
   * have match() executed against it.
   *
   * @param workflowLanguage the language the workflow was written in
   * @param workflow the contents of the workflow
   * @param accessoryFiles additional files that should be included with the workflow
   * @param vidarrId the ID of the workflow being executed
   * @param workflowParameters the input parameters to the workflow
   * @param engineParameters the input configuration parameters to the workflow engine
   * @return the initial state of the provision out process
   */
  State prepareInput(
      WorkflowLanguage workflowLanguage,
      String workflow,
      Stream<Pair<String, String>> accessoryFiles,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters);

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
