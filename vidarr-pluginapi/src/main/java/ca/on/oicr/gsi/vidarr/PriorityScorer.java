package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.PriorityScorer.PriorityScorerIdResolver;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import io.undertow.server.HttpHandler;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

/**
 * Examine a score to determine if a workflow run should be allowed to proceed or wait
 */
@JsonTypeIdResolver(PriorityScorerIdResolver.class)
@JsonTypeInfo(use = Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface PriorityScorer {

  final class PriorityScorerIdResolver extends TypeIdResolverBase {

    private final Map<String, Class<? extends PriorityScorer>> knownIds =
        ServiceLoader.load(PriorityScorerProvider.class).stream()
            .map(Provider::get)
            .flatMap(PriorityScorerProvider::types)
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
   * Determine if the workflow run can begin now
   *
   * @param workflowName        the name of the workflow
   * @param workflowVersion     the version of the workflow
   * @param vidarrId            the workflow run identifier
   * @param created             the time when the workflow run was first submitted
   * @param workflowMaxInFlight the max-in-flight value for this workflow, if available
   * @param score               the computed score for this workflow run
   * @return true if the workflow run may proceed; false if it should wait
   */
  boolean compute(
      String workflowName,
      String workflowVersion,
      String vidarrId,
      Instant created,
      OptionalInt workflowMaxInFlight,
      int score);

  /**
   * Priority scorers may provide an optional HTTP API to extend their functionality that will be
   * accessible through their parent consumable resource.
   *
   * <p>Paths will be automatically prefixed with the instance's name, so the HTTP handler can
   * assume it is at the root URL (<i>i.e.,</i> <code>/</code>).
   *
   * @return the HTTP handler to use
   */
  Optional<HttpHandler> httpHandler();

  /**
   * Indicate that Vidarr has restarted and this workflow run will be started even if this scorer
   * would make it wait.
   *
   * @param workflowName    the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId        the identifier of the workflow run
   */
  void recover(String workflowName, String workflowVersion, String vidarrId);

  /**
   * Indicate that the workflow run has completed and the score should purge any state about it
   *
   * @param workflowName    the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId        the identifier of the workflow run
   */
  void complete(String workflowName, String workflowVersion, String vidarrId);


  /**
   * Indicate that an outer aggregate scorer has failed to meet criteria to start this workflow run,
   * don't completely forget this job but also don't keep it at top priority.
   *
   * @param workflowName    the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId        the identifier of the workflow run
   */
  void putItBack(String workflowName, String workflowVersion, String vidarrId);


  /**
   * Perform any initialization required by this input
   */
  void startup();
}
