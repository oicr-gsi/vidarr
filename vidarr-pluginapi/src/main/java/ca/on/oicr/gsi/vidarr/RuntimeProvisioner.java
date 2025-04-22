package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
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
 *
 * @param <State> the state information used for provisioning out data
 */
@JsonTypeIdResolver(RuntimeProvisioner.RuntimeProvisionerIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface RuntimeProvisioner<State extends Record> {

  final class RuntimeProvisionerIdResolver extends TypeIdResolverBase {

    private final Map<String, Class<? extends RuntimeProvisioner<? extends Record>>> knownIds =
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
   * Prepare state to provision out data
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link #build()} so that Vidarr can execute it once the database is in a healthy state.
   * If build() is compared to Pattern.compile(), then prepareProvisionInput prepares the String
   * that will have match() executed against it.
   *
   * @param workflowRunUrl the URL provided by the {@link WorkflowEngine.Result#workflowRunUrl()}
   * @return the state that will be used by {@link #build()}
   */
  State prepareProvisionInput(String workflowRunUrl);

  /**
   * Build a declarative structure to execute the provisioning when played back
   *
   * Compare to Pattern.compile() - builds a ready object for later use by the processor.
   * This allows the processor to play back and pause the sequence of events built here.
   *
   * @return the sequence of operations that should be performed
   */
  OperationAction<?, State, OutputProvisioner.Result> build();

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
