package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import ca.on.oicr.gsi.vidarr.LogFileStasher;
import ca.on.oicr.gsi.vidarr.LogFileStasherProvider;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Gauge;
import java.io.IOException;
import java.lang.module.Configuration;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

public class CromwellLogFileStasher implements LogFileStasher {
  static final HttpClient CLIENT =
      HttpClient.newBuilder()
          .version(Version.HTTP_1_1)
          .followRedirects(Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(20))
          .build();
  @JsonIgnore private SSHClient client;
  private String hostname, username;
  private short port;
  @JsonIgnore private SFTPClient sftp; // Required for symlink() method
  static final ObjectMapper MAPPER = new ObjectMapper();
  private String lokiUrl;
  private int lines;
  private Path fileName; // File being monitored (TODO: Needs further discovery)

  // ------------------------------------------------------------------------------------------------
  /* THE FOLLOWING VAR ORIGINATE FROM THE SHESMU LOKI PLUGIN (for reference) */
  /* QUESTION: I assume the following error, writeLatency and writeTime have to do with metrics
   * of pushing data to Loki. Unsure if these are necessary for this plugin. */
  // --> DON'T WORRY ABOUT IT FOR NOW
  private static final Gauge error =
      Gauge.build(
              "cromwell_loki_push_error",
              "Whether the Loki client had a push error on its last write")
          .labelNames("filename")
          .register();
  private static final LatencyHistogram writeLatency =
      new LatencyHistogram(
          "cromwell_loki_write_latency",
          "The amount of time it took to push data to Loki",
          "filename");
  private static final Gauge writeTime =
      Gauge.build(
              "cromwell_loki_write_time", "The last time Cromwell attempted to push data to Loki")
          .labelNames("filename")
          .register();
  private final Pattern INVALID_LABEL = Pattern.compile("[^a-zA-Z0-9_]");
  /* QUESTION: From what I can see, this buffer is only written to when writing out a log message
   * in the Loki plugin. Unsure how/if this is relevant to this provisioner. */
  private final Map<Map<String, String>, List<Pair<Instant, String>>> buffer = new HashMap<>();
  /* QUESTION: I assume that this is the configuration required by the Loki plugin in order for Shesmu
   * to actually interact with Loki? */
  private Optional<Configuration> configuration = Optional.empty();
  /* END VAR FROM SHESMU LOKI PLUGIN */
  // ------------------------------------------------------------------------------------------------

  public static LogFileStasherProvider provider() {
    return () -> Stream.of(new Pair<>("cromwell", CromwellLogFileStasher.class));
  }

  public void startup() {
    try {
      client = new SSHClient();
      client.loadKnownHosts();
      client.addHostKeyVerifier(new PromiscuousVerifier());
      client.connect(hostname, port);
      client.authPublickey(username);
      sftp = client.newSFTPClient();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write a log file to better storage
   *
   * <p>Log the number of configured lines to the given Loki instance
   *
   * @param vidarrId the Vidarr workflow run id
   * @param monitor a monitor to allow asynchronous execution
   * @param logFile the path to the log file
   * @param labels any extra labels that should be added to the log store, if applicable
   */
  @Override
  public void stash(
      String vidarrId, StashMonitor monitor, String logFile, Map<String, String> labels) {
    // CHECK: My assumption was that this stash method essentially replaces the provision method
    // that was in the RP. --> close enough

    // QUESTION: Is the block below (recording the CallLogState) necessary (since we're not
    // returning a state)? --> There is no state, don't have to keep track of it
    // --> assume loki will dedup for us! :D Oterhwise, stash monitor will need redesigning
    //    final var state = new CallLogState();
    //    state.setLog(logFile);
    //    // QUESTION: Unsure as to what index is responsible for --> tracks our place in logging
    // Bunch of call entries in debug info
    //    state.setIndex(0);
    //    state.setKind(Kind.STDERR);
    //    state.setLabels(labels);

    // Doesn't need to be in a scheduleTask unless we actually intend to wait
    // If we want this plugin to be recovereable, THEN this would be wrapped
    monitor.scheduleTask(
        () -> {
          // CHECK: I'm assuming the same error checking will be needed? (Check that the file size
          // isn't atrociously large, etc.) --> YES
          byte[] content = new byte[1000];
          byte[] handle = "stderrFile".getBytes();
          RemoteFile stderrFile = new RemoteFile(sftp.getSFTPEngine(), logFile, handle);
          try {
            // Read the contents from the stderrFile into content variable
            stderrFile.new RemoteFileInputStream()
                .read(content, ((int) stderrFile.length() - 1000), 1000);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

          final var body = MAPPER.createObjectNode();
          final HttpRequest post;
          try {
            // Writing the complete log out to Loki in JSON. Not ideal!
            // Alternatively: Rather than loop that reads all the data, create a custom body
            // publisher
            /* Java HTTP library requests for more data as appropriate. Can incrementally provide that info
             * Subscriber interface?
             * Build one body publisher, reads out of SFTP
             * Loops, calls method on the subscriber (HTTP interface, gets more data!) */
            post =
                HttpRequest.newBuilder(URI.create(lokiUrl))
                    .POST(BodyPublishers.ofString(MAPPER.writeValueAsString(labels)))
                    .header("Content-type", "application/json")
                    .build();
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
          // QUESTION: The fileName variable being called here is originally from the NiassaOP and
          // was set to the
          writeTime.labels(fileName.toString()).setToCurrentTime();
          try (final var timer = writeLatency.start(fileName.toString())) {
            final var response = CLIENT.send(post, BodyHandlers.ofString());
            final var success = response.statusCode() / 100 == 2;
            if (success) {
              buffer.clear();
              error.labels(fileName.toString()).set(0);
            } else {
              try (final var sc = new Scanner(response.body())) {
                sc.useDelimiter("\\A");
                if (sc.hasNext()) {
                  final var message = sc.next();
                  if (message.contains("ignored")) {
                    buffer.clear();
                    // QUESTION: In the Shesmu Loki plugin file, it says "Loki complains if we send
                    // duplicate messages, so treat that like success". Would it be safe to assume
                    // then that Loki does not handle deduping? TODO: Does Loki handle deduping?
                    // --> We will assume for now that Loki DOES handle dedup
                    error.labels(fileName.toString()).set(0);
                    return;
                  }
                  System.err.println(message);
                }
              }
              error.labels(fileName.toString()).set(1);
            }
          } catch (final Exception e) {
            e.printStackTrace();
            error.labels(fileName.toString()).set(1);
          }
        });
  }

  public String getLokiUrl() {
    return lokiUrl;
  }

  public void setLokiUrl(String lokiUrl) {
    this.lokiUrl = lokiUrl;
  }

  public int getLines() {
    return lines;
  }

  public void setLines(int lines) {
    this.lines = lines;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public short getPort() {
    return port;
  }

  public void setPort(short port) {
    this.port = port;
  }
}
