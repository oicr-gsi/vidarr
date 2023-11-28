package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.PriorityInput.PriorityInputIdResolver;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import io.undertow.server.HttpHandler;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

/**
 * Consume data from a submission request to prepare a number for use in the priority scoring
 * consumable resource.
 */
@JsonTypeIdResolver(PriorityInputIdResolver.class)
@JsonTypeInfo(use = Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface PriorityInput {

  final class PriorityInputIdResolver extends TypeIdResolverBase {

    private final Map<String, Class<? extends PriorityInput>> knownIds =
        ServiceLoader.load(PriorityInputProvider.class).stream()
            .map(Provider::get)
            .flatMap(PriorityInputProvider::types)
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
   * Compute the integer priority for the submission request
   *
   * @param workflowName the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param created the time the workflow was first submitted
   * @param input the data included as part of the submission
   * @return the score
   */
  int compute(String workflowName, String workflowVersion, Instant created, JsonNode input);

  /**
   * Priority inputs may provide an optional HTTP API to extend their functionality that will be
   * accessible through their parent consumable resource.
   *
   * <p>Paths will be automatically prefixed with the instance's name, so the HTTP handler can
   * assume it is at the root URL (<i>i.e.,</i> <code>/</code>).
   *
   * @return the HTTP handler to use
   */
  Optional<HttpHandler> httpHandler();

  /**
   * The type of data that must be provided as part of the submission request
   *
   * @return the type of data required
   */
  BasicType inputFromSubmitter();

  /**
   * Perform any initialization required by this input
   *
   * @param resourceName the name of the consumable resource that ultimately owns this input
   * @param inputName a unique identifier; depending on the configuration, this name may not be a
   *     valid Shesmu identifier. It should only be used for logging/caching purposes.
   */
  void startup(String resourceName, String inputName);
}
