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
 * <p>It does not operate on individual output from a workflow run, but on the workflow run itself,
 * provided by an identifier that connects it to the workflow engine that ran it. This distinguishes
 * the RuntimeProvisioner from the OutputProvisioner - RuntimeProvisioners run once per workflow
 * run; OutputProvisioners run once per output file (potentially many times per workflow run). Both
 * are called at the end of the RUNNING phase.
 *
 * <p>RuntimeProvisioner uses jackson-databind to map information from the server's '.vidarrconfig'
 * file to member non-static fields. The @JsonIgnore annotation prevents this.
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

  /** Write configuration information to the Vidarr status page. */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /**
   * The unique name of this plugin. Required for journaling state to database and crash recovery.
   */
  String name();

  /**
   * Begin provisioning out a new output
   *
   * @param workflowRunUrl the URL provided by the {@link WorkflowEngine.Result#workflowRunUrl()}
   * @param monitor WorkMonitor for writing the output of the checking process and scheduling
   *     asynchronous tasks. OutputProvisioner.Result is the expected output type. JsonNode is the
   *     format of the state records.
   * @return JsonNode used by WrappedMonitor in BaseProcessor.Phase3Run to serialize to the database
   */
  JsonNode provision(
      String workflowRunUrl, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor);

  /**
   * Restart a provisioning process from state saved in the database
   *
   * <p>Rebuild state from `state` object then schedule appropriate next step with
   * `monitor.scheduleTask()`
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the provisioning process
   */
  void recover(JsonNode state, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor);

  /**
   * Called to initialise this runtime provisioner.
   *
   * <p>Actual reading of configuration files does not need to be done, due to jackson-databind
   * populating member fields automatically.
   *
   * <p>If the configuration is invalid, this should throw a runtime exception.
   */
  void startup();
}
