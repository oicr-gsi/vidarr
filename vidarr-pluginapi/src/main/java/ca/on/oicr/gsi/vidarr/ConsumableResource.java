package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

/** A broker that can block workflows from starting by managing their resource footprints */
@JsonTypeIdResolver(ConsumableResource.ConsumableResourceIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface ConsumableResource {
  final class ConsumableResourceIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends ConsumableResource>> knownIds =
        ServiceLoader.load(ConsumableResourceProvider.class).stream()
            .map(Provider::get)
            .flatMap(ConsumableResourceProvider::types)
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
   * To operate, this resource requires the submission request to include a parameter.
   *
   * @return the name of the parameter and the type required, or an empty value if no input is
   *     required.
   */
  Optional<Pair<String, BasicType>> inputFromSubmitter();

  /**
   * Indicate that Vidarr has restarted and it is reasserting an old claim.
   *
   * @param workflowName the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId the identifier of the workflow run
   * @param resourceJson the consumable resource information stored in db, if applicable and provided.
   */
  void recover(String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> resourceJson);

  /**
   * Indicate that the workflow is done with this resource
   *
   * @param workflowName the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId the identifier of the workflow run
   * @param input the consumable resource information requested from the submitter, if applicable and provided
   *        input will only be provided if the workflow run has not completed
   */
  void release(String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input);

  /**
   * Request the resource
   *
   * @param workflowName the name of the workflow
   * @param workflowVersion the version of the workflow
   * @param vidarrId the identifier of the workflow run
   * @param input the consumable resource information requested from the submitter, if applicable and provided.
   * @return whether this resource is available
   */
  ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input);


  /**
   * Called to initialise this consumable resource.
   *
   * <p>If the configuration is invalid, this should throw a runtime exception.
   *
   * @param name
   */
  void startup(String name);

  /**
   * Called to determine if this consumable resource must be provided as part of the input.
   *.
   */
  boolean isInputFromSubmitterRequired();
}
