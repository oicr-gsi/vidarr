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

/** A mechanism to collect output data from a workflow and push it into an appropriate data store */
@JsonTypeIdResolver(InputProvisioner.InputProvisionerIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface InputProvisioner {
  final class InputProvisionerIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends InputProvisioner>> knownIds =
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
   * Begin provisioning out a new input that was registered in Vidarr
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link WorkMonitor#scheduleTask(Runnable)} callback so that Vidarr can execute it once the
   * database is in a healthy state.
   *
   * @param language the workflow language the output will be consumed by
   * @param id the Vidarr ID for the file
   * @param path the output path registered in Vidarr for the file
   * @param monitor the monitor structure for writing the output of the provisioning process
   * @return the initial state of the provision out process
   */
  JsonNode provision(
      WorkflowLanguage language, String id, String path, WorkMonitor<JsonNode, JsonNode> monitor);

  /**
   * Begin provisioning out a new input that was not registered in Vidarr
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link WorkMonitor#scheduleTask(Runnable)} callback so that Vidarr can execute it once the
   * database is in a healthy state.
   *
   * @param language the workflow language the output will be consumed by
   * @param metadata the information coming from the submitter to direct provisioning
   * @param monitor the monitor structure for writing the output of the provisioning process
   * @return the initial state of the provision out process
   */
  JsonNode provisionExternal(
      WorkflowLanguage language, JsonNode metadata, WorkMonitor<JsonNode, JsonNode> monitor);

  /**
   * Restart a provisioning process from state saved in the database
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link WorkMonitor#scheduleTask(Runnable)} callback so that Vidarr can execute it once the
   * database is in a healthy state.
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the provisioning process
   */
  void recover(JsonNode state, WorkMonitor<JsonNode, JsonNode> monitor);
  /**
   * Called to initialise this input provisioner.
   *
   * <p>If the configuration is invalid, this should throw a runtime exception.
   */
  void startup();
}
