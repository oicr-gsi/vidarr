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
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.xml.stream.XMLStreamException;

/**
 * A mechanism to collect output data from a workflow and push it into an appropriate data store
 *
 * <p>OutputProvisioners run once per output file for a workflow run. This is different from the
 * RuntimeProvisioner, which executes once per workflow run. Both are called at the end of the
 * RUNNING phase.
 *
 * <p>OutputProvisioner uses jackson-databind to map information from the server's '.vidarrconfig'
 * file to member non-static fields. The @JsonIgnore annotation prevents this.
 *
 * @param <PreflightState> the state information used to run the preflight check
 * @param <State> the state information used for provisioning out data
 */
@JsonTypeIdResolver(OutputProvisioner.OutputProvisionerIdResolver.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, include = As.PROPERTY, property = "type")
public interface OutputProvisioner<PreflightState extends Record, State extends Record> {
  final class OutputProvisionerIdResolver extends TypeIdResolverBase {
    private final Map<String, Class<? extends OutputProvisioner<?, ?>>> knownIds =
        ServiceLoader.load(OutputProvisionerProvider.class).stream()
            .map(Provider::get)
            .flatMap(OutputProvisionerProvider::types)
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
  /** Visit the output of the provisioning process */
  interface ResultVisitor {

    /**
     * The output is a file
     *
     * @param storagePath the permanent storage path for the file
     * @param md5 the MD5 sum of the file as a hex-encoded string
     * @param size the size of the file, in bytes
     * @param metatype the MIME type of the file
     */
    void file(String storagePath, String md5, long size, String metatype);

    /**
     * The output is a reference to another system *
     *
     * @param url the URL that describes how to find the output in the remote system
     */
    void url(String url, Map<String, String> labels);
  }

  /** Output information about successful provisioning */
  abstract class Result {
    /**
     * Create metadata for a file object
     *
     * @param storagePath the permanent storage path for the file
     * @param md5 the MD5 sum of the file as a hex-encoded string
     * @param size the size of the file, in bytes
     * @param metatype the MIME type of the file
     * @return the output provisioning metadata
     */
    public static Result file(String storagePath, String md5, long size, String metatype) {
      return new Result() {

        @Override
        public void visit(ResultVisitor visitor) {
          visitor.file(storagePath, md5, size, metatype);
        }
      };
    }

    /**
     * Create metadata that refers to a remote system
     *
     * @param url the URL that describes how to find the output in the remote system
     * @return the output provisioning metadata
     */
    public static Result url(String url, Map<String, String> labels) {
      return new Result() {
        private final Map<String, String> label = new TreeMap<>(labels);

        @Override
        public void visit(ResultVisitor visitor) {
          visitor.url(url, Collections.unmodifiableMap(label));
        }
      };
    }

    private Result() {}

    /**
     * Visit the output metadata of the provisioning process
     *
     * @param visitor the visitor to view the result
     */
    public abstract void visit(ResultVisitor visitor);
  }

  /** Checks if the provisioner can handle this type of data */
  boolean canProvision(OutputProvisionFormat format);

  /** Display configuration status */
  void configuration(SectionRenderer sectionRenderer) throws XMLStreamException;

  /**
   * Prepare state to check that the metadata provided by the submitter is valid.
   *
   * @param metadata the metadata provided by the submitter
   */
  PreflightState preflightCheck(JsonNode metadata);

  /**
   * Prepare state to provision out data
   *
   * <p>This method should not do any externally-visible work. Anything it needs should be done in a
   * {@link #run()} so that Vidarr can execute it once the database is in a healthy state.
   *
   * @param workflowRunId the workflow run ID assigned by Vidarr
   * @param data the output coming from the workflow
   * @param metadata the information coming from the submitter to direct provisioning
   * @return the initial state of the provision out process
   */
  State provision(String workflowRunId, String data, JsonNode metadata);

  /**
   * Create a declarative structure to execute the provisioning
   *
   * @return the sequence of operations that should be performed
   */
  OperationAction<?, State, Result> run();

  /**
   * Create a declarative structure to execute the pre-flight check
   *
   * @return the sequence of operations that should be performed
   */
  OperationAction<?, PreflightState, Boolean> runPreflight();

  /**
   * Called to initialise this output provisioner.
   *
   * <p>If the configuration is invalid, this should throw a runtime exception.
   */
  void startup();

  /** Get the name for this configuration in JSON files */
  String type();

  /**
   * Get the type of information required for provisioning out the files
   *
   * @param format the input format
   * @return the metadata that the client must supply to be able to provision in this data
   */
  BasicType typeFor(OutputProvisionFormat format);
}
