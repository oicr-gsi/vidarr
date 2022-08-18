package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import ca.on.oicr.gsi.vidarr.LogFileStasher;
import ca.on.oicr.gsi.vidarr.LogFileStasherProvider;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Gauge;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.module.Configuration;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import net.schmizz.sshj.SSHClient;
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
  private static Semaphore sftpSemaphore = new Semaphore(1);
  private String hostname, username;
  private short port;
  @JsonIgnore private SFTPClient sftp; // Required for symlink() method
  static final ObjectMapper MAPPER = new ObjectMapper();
  private String lokiUrl;
  private int lines;

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
      String vidarrId, StashMonitor monitor, String logFile, Map<String, String> labels)
      throws IOException {
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
    // If we want this plugin to be recoverable, THEN this would be wrapped
    // CHECK: I'm assuming the same error checking will be needed? (Check that the file size
    // isn't atrociously large, etc.) --> YES

    // Use sftp client to create a symlink to the target file we wish to log.
    try {
      sftpSemaphore.acquire();
      try {
        sftp.mkdirs(vidarrId); // Use the vidarr ID as the target directory
        sftp.symlink(logFile, vidarrId);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Failed to provision \"%s\" to \"%s\".", logFile, vidarrId), e);
      } finally {
        sftpSemaphore.release();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (Files.size(Path.of(vidarrId)) >= Math.pow(10, 12)) {
      throw new RuntimeException(
          String.format("File \"%s\" is too large to log to Loki. Size exceeds 1TB", logFile));
    }

    // In order to push to Loki, we create a JSON post body of the log entries and push the body to
    // Loki. See http://grafana.com/docs/loki/latest/api/#push-log-entries-to-loki
    final var body = MAPPER.createObjectNode();
    // QUESTION: The streams object we're creating has an array of entries. What are these entries
    // and where are they coming from?
    final var streams = body.putArray("streams");
    final var streamsEntry = streams.addObject();
    final var stream = streamsEntry.putObject("stream");
    // Insert the given labels into the new JSON post body
    for (final var labelEntry : labels.entrySet()) {
      final var label = stream.put(labelEntry.getKey(), labelEntry.getValue());
    }
    // We will read the file using the created symlink line by line
    final var values = streamsEntry.putArray("values");
    StringBuilder sb = new StringBuilder();
    try (BufferedReader buffer = new BufferedReader(new FileReader(vidarrId))) {
      String line;
      while ((line = buffer.readLine()) != null) {
        // Each entry in the values array is another array in itself, logging 1) the unix epoch in
        // nanoseconds, and 2) the actual line itself
        final var valuesEntry = values.addArray();
        valuesEntry.add(String.format("%d%09d", Instant.now().getNano(), line));
      }
    }

    final HttpRequest post;
    try {
      // Writing the complete log out to Loki in JSON. Not ideal!
      // Alternatively: Rather than loop that reads all the data, create a custom body publisher
      /* Java HTTP library requests for more data as appropriate. Can incrementally provide that info
       * Subscriber interface?
       * Build one body publisher, reads out of SFTP
       * Loops, calls method on the subscriber (HTTP interface, gets more data!) */
      post =
          HttpRequest.newBuilder(URI.create(String.format("%s/loki/api/v1/push", lokiUrl)))
              .POST(BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
              .header("Content-Type", "application/json")
              .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
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
