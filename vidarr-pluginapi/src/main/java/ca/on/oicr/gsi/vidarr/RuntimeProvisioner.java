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
 * A mechanism to collect metrics and log data from a workflow and push it into an appropriate data
 * store
 *
 * <p>It does not operate on individual output from a workflow run, but simply on the workflow run
 * itself, provided by an identifier that connects it to the workflow engine that ran it.
 */
@JsonTypeIdResolver(RuntimeProvisioner.RuntimeProvisionerIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface RuntimeProvisioner {
  final class RuntimeProvisionerIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends RuntimeProvisioner>> knownIds =
        ServiceLoader.load(RuntimeProvisionerProvider.class).stream()
            .map(Provider::get)
            .flatMap(RuntimeProvisionerProvider::types)
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
  /** Display configuration status */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /** The name of this plugin */
  String name();

  /**
   * Begin provisioning out a new output
   *
   * @param workflowRunUrl the URL provided by the {@link WorkflowEngine.Result#workflowRunUrl()}
   * @param monitor the monitor structure for writing the output of the checking process
   */
  JsonNode provision(
      String workflowRunUrl, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor);

  /**
   * Restart a provisioning process from state saved in the database
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the provisioning process
   */
  void recover(JsonNode state, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor);

  /**
   * Called to initialise this runtime provisioner.
   *
   * <p>If the configuration is invalid, this should throw a runtime exception.
   */
  void startup();
}
