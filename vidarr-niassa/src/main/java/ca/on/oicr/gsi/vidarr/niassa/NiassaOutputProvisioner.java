package ca.on.oicr.gsi.vidarr.niassa;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

/**
 * Takes in information about Niassa files and symlinks them to a target directory received from
 * Shesmu. Also copies information about said files to Vidarr database.
 */
public class NiassaOutputProvisioner implements OutputProvisioner {
  /**
   * Provides access to Jackson json interpretation methods. Not private so
   * NiassaOutputProvisionerProvider has access as well.
   */
  static final ObjectMapper MAPPER = new ObjectMapper();
  /**
   * For cases where we need to substitute one file type for another. This is for consistency in
   * formatting between types and to fix misspellings.
   */
  private static final Map<String, String> SUBSTITUTIONS =
      Map.ofEntries(
          Map.entry("application/txt-gz", "application/text-gz"),
          Map.entry("application/vcf-4-gzip", "application/vcf-gz"),
          Map.entry("txt/junction", "text/junction"),
          Map.entry("txt/plain", "text/plain"));

  private static Semaphore sftpSemaphore = new Semaphore(1);

  public static OutputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("niassa", NiassaOutputProvisioner.class));
  }
  /**
   * If we recreated the source directory structure, it'd be way too deep. Chunk the file path and
   * rebuild the path by appending this information to the target directory defined in the plugin
   * configuration. For an example of this structure, please look at the .git directory.
   */
  private int[] chunks;
  /** Manages SSH connection information. */
  @JsonIgnore private SSHClient client;

  private String hostname;
  private short port;
  /** Required specifically for its symlink() method. */
  @JsonIgnore private SFTPClient sftp;

  private String username;
  /**
   * Set up the NiassaOutputProvisioner with a file path chunking and the SSH connection information
   * provided by the plugin configuration. This constructor should only be called by
   * NiassaOutputProvisionerProvider.readConfiguration().
   *
   * @throws IOException when SSH connection fails
   */
  public NiassaOutputProvisioner() throws IOException {}

  @Override
  public boolean canProvision(OutputProvisionFormat format) {
    return format == OutputProvisionFormat.FILES;
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    // skip
  }

  public int[] getChunks() {
    return chunks;
  }

  public String getHostname() {
    return hostname;
  }

  public short getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  @Override
  public JsonNode preflightCheck(JsonNode metadata, WorkMonitor<Boolean, JsonNode> monitor) {
    monitor.scheduleTask(() -> monitor.complete(true));
    return null;
  }

  @Override
  public void preflightRecover(JsonNode state, WorkMonitor<Boolean, JsonNode> monitor) {
    monitor.scheduleTask(() -> monitor.complete(true));
  }

  /**
   * Build the path by appending the chunks to the TARGET directory provided by shesmu. Then use the
   * SFTPClient to symlink from the SOURCE to the TARGET. Log file metadata to the Vidarr database
   * with monitor.complete()
   *
   * @param workflowRunId the workflow run ID assigned by Vidarr
   * @param data this is a json object inside a string, containing labels, md5, fileSize, path, and
   *     metatype. This comes from NiassaWorkflowEngine.
   * @param metadata the information coming from the submitter to direct provisioning. This is a
   *     jsonobject which contains root of TARGET. Append chunks to this. This is the
   *     'outputDirectory' defined in typeFor and comes from shesmu
   * @param monitor the monitor structure for writing the output of the provisioning process
   * @return nullNode as monitor.complete() does all the work needed for this plugin.
   */
  @Override
  public JsonNode provision(
      String workflowRunId, String data, JsonNode metadata, WorkMonitor<Result, JsonNode> monitor) {
    monitor.scheduleTask(
        () -> {
          // Set up all the data in the formats we need
          JsonNode dataAsJson;
          try {
            dataAsJson = MAPPER.readTree(data);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          String sourcePath = dataAsJson.get("path").asText();
          Path targetPath = Path.of(metadata.get("outputDirectory").asText());

          // Append chunks to target directory by repeatedly resolving path with chunk added
          int startIndex = 0;
          for (final int length : chunks) {
            if (length < 1) break;
            final int endIndex = Math.min(workflowRunId.length(), startIndex + length);
            if (endIndex == startIndex) break;
            targetPath = targetPath.resolve(workflowRunId.substring(startIndex, endIndex));
            startIndex = endIndex;
          }
          // Append the entire workflow run ID as a directory
          targetPath = targetPath.resolve(workflowRunId);

          // Use the sftp client to create symlink to the TARGET
          try {
            sftpSemaphore.acquire();
            try {
              sftp.mkdirs(targetPath.toString());

              // Be explicit about the target filename - sftp just says "failure" otherwise
              String[] sourcePathSplit = sourcePath.split("/");
              targetPath = targetPath.resolve(sourcePathSplit[sourcePathSplit.length - 1]);

              sftp.symlink(sourcePath, targetPath.toString());
            } catch (IOException e) {
              throw new RuntimeException(
                  String.format("Failed to provision \"%s\" to \"%s\".", sourcePath, targetPath),
                  e);
            } finally {
              sftpSemaphore.release();
            }
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }

          // When done, monitor.complete with Result of type file describing symlinked file
          String metatype = dataAsJson.get("metatype").asText();
          monitor.complete(
              Result.file(
                  targetPath.toString(),
                  dataAsJson.get("md5").asText(),
                  dataAsJson.get("fileSize").asLong(),
                  SUBSTITUTIONS.getOrDefault(metatype, metatype)));
        });

    // Return nothing
    return MAPPER.nullNode();
  }

  /**
   * Schedule a task that fails (permanentFailure) because we don't need to recover from database
   * state in this plugin.
   *
   * @param state the frozen database state
   * @param monitor the monitor structure for writing the output of the provisioning process
   */
  @Override
  public void recover(JsonNode state, WorkMonitor<Result, JsonNode> monitor) {
    monitor.scheduleTask(() -> monitor.permanentFailure("Dummy action."));
  }

  /** File path chunking to be appended to the target received from Shesmu */
  public void setChunks(int[] chunks) {
    this.chunks = chunks;
  }

  /** Hostname for SSH connection from plugin configuration */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /** Port for SSH connection from plugin configuration */
  public void setPort(short port) {
    this.port = port;
  }

  /** Username for SSH connection from plugin configuration */
  public void setUsername(String username) {
    this.username = username;
  }

  @Override
  public void startup() {
    try {
      client = new SSHClient();
      client.loadKnownHosts();
      client.addHostKeyVerifier(new PromiscuousVerifier());
      client.connect(hostname, port);
      client.authPublickey(username);
      sftp = client.newSFTPClient();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public String type() {
    return "niassa";
  }

  @Override
  public BasicType typeFor(OutputProvisionFormat format) {
    if (format == OutputProvisionFormat.FILES) {
      return BasicType.object(new Pair<>("outputDirectory", BasicType.STRING));
    } else {
      throw new IllegalArgumentException("Cannot provision non-file output");
    }
  }
}
