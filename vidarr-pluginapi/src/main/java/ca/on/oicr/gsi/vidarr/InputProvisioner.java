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
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;
import javax.xml.stream.XMLStreamException;

/**
 * A mechanism to collect output data from a workflow and push it into an appropriate data store
 *
 * @param <State> the state information used for provisioning in data
 */
@JsonTypeIdResolver(InputProvisioner.InputProvisionerIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface InputProvisioner<State extends Record> {
  final class InputProvisionerIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends InputProvisioner<?>>> knownIds =
        ServiceLoader.load(InputProvisionerProvider.class).stream()
            .map(Provider::get)
            .flatMap(InputProvisionerProvider::types)
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
  /** Checks if the provisioner can handle this type of data */
  boolean canProvision(InputProvisionFormat format);

  /** Display configuration status */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /**
   * Get the type of information required for provisioning external files
   *
   * @param format the input format
   * @return the metadata that the client must supply to be able to provision in this data
   */
  BasicType externalTypeFor(InputProvisionFormat format);

  /**
   * Prepare state to provision in data that was registered in Vidarr
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link #build()} so that Vidarr can execute it once the database is in a healthy state.
   * If build() is compared to Pattern.compile(), then prepareExternalProvisionInput prepares the String
   * that will have match() executed against it.
   *
   * @param language the workflow language the output will be consumed by
   * @param id the Vidarr ID for the file
   * @param path the output path registered in Vidarr for the file
   * @return the initial state of the provision in process
   */
  State prepareInternalProvisionInput(WorkflowLanguage language, String id, String path);

  /**
   * Prepare state to provision in data that was not registered in Vidarr
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link #build()} so that Vidarr can execute it once the database is in a healthy state.
   * If build() is compared to Pattern.compile(), then prepareExternalProvisionInput prepares the String
   * that will have match() executed against it.
   *
   * @param language the workflow language the output will be consumed by
   * @param metadata the information coming from the submitter to direct provisioning
   * @return the initial state of the provision in process
   */
  State prepareExternalProvisionInput(WorkflowLanguage language, JsonNode metadata);

  /**
   * Build a declarative structure to execute the provisioning when played back
   *
   * Compare to Pattern.compile() - builds a ready object for later use by the processor.
   * This allows the processor to play back and pause the sequence of events built here.
   * @return the sequence of operations that should be performed
   */
  OperationAction<?, State, JsonNode> build();
  /**
   * Called to initialise this input provisioner.
   *
   * <p>If the configuration is invalid, this should throw a runtime exception.
   */
  void startup();
}
