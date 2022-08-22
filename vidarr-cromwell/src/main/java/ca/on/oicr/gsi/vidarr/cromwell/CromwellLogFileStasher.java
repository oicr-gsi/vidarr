package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import ca.on.oicr.gsi.vidarr.LogFileStasher;
import ca.on.oicr.gsi.vidarr.LogFileStasherProvider;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
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
  static final ObjectMapper MAPPER = new ObjectMapper();
  // ------------------------------------------------------------------------------------------------
  private static final Gauge error =
      Gauge.build(
              "cromwell_loki_push_error",
              "Whether the Loki client had a push error on its last write")
          .labelNames("filename")
          .register();
  private static final Semaphore sftpSemaphore = new Semaphore(1);
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

  public static LogFileStasherProvider provider() {
    return () -> Stream.of(new Pair<>("cromwell", CromwellLogFileStasher.class));
  }

  private final Pattern INVALID_LABEL = Pattern.compile("[^a-zA-Z0-9_]");
  /* QUESTION: From what I can see, this buffer is only written to when writing out a log message
   * in the Loki plugin. Unsure how/if this is relevant to this provisioner. */
  private final Map<Map<String, String>, List<Pair<Instant, String>>> buffer = new HashMap<>();
  @JsonIgnore private SSHClient client;
  /* QUESTION: I assume that this is the configuration required by the Loki plugin in order for Shesmu
   * to actually interact with Loki? */
  private final Optional<Configuration> configuration = Optional.empty();
  private String hostname, username;
  private int lines;
  private String lokiUrl;
  private short port;
  /* END VAR FROM SHESMU LOKI PLUGIN */
  // ------------------------------------------------------------------------------------------------
  @JsonIgnore private SFTPClient sftp; // Required for symlink() method

  public String getHostname() {
    return hostname;
  }

  public int getLines() {
    return lines;
  }

  public String getLokiUrl() {
    return lokiUrl;
  }

  public short getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public void setLines(int lines) {
    this.lines = lines;
  }

  public void setLokiUrl(String lokiUrl) {
    this.lokiUrl = lokiUrl;
  }

  public void setPort(short port) {
    this.port = port;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void startup() {
    try {
      client = new SSHClient();
      client.loadKnownHosts();
      client.addHostKeyVerifier(new PromiscuousVerifier());
      client.connect(hostname, port);
      client.authPublickey(username);
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

    // Check that the file is not atrociously large
    if (Files.size(Path.of(logFile)) >= Math.pow(10, 12))
      throw new RuntimeException(
          String.format("File \"%s\" is too large to log to Loki. Size exceeds 1TB", logFile));

    // In order to push to Loki, we create a JSON post body of the log entries and push the body to
    // Loki. See http://grafana.com/docs/loki/latest/api/#push-log-entries-to-loki
    final var body = MAPPER.createObjectNode();
    // QUESTION: The streams object we're creating has an array of entries. What are these entries
    // and where are they coming from?
    final var streams = body.putArray("streams");
    // Insert the given labels into the new JSON post body
    for (final var labelEntry : labels.entrySet()) {
      final var streamsEntry = streams.addObject();
      final var stream = streamsEntry.putObject("stream");
      stream.put(labelEntry.getKey(), labelEntry.getValue());
      final var values = streamsEntry.putArray("values");
      try (BufferedReader buffer = new BufferedReader(new FileReader(logFile))) {
        String line;
        while ((line = buffer.readLine()) != null) {
          final var valuesEntry = values.addArray();
          valuesEntry.add(
              String.format("%d%09d", Instant.now().getEpochSecond(), Instant.now().getNano()));
          valuesEntry.add(line.replace('\n', ' '));
        }
      }
    }
    // Create post request to log the lines to Loki

    // Writing the complete log out to Loki in JSON. Not ideal!
    // Alternatively: Rather than loop that reads all the data, create a custom body publisher
    /* Java HTTP library requests for more data as appropriate. Can incrementally provide that
    info
     * Subscriber interface?
     * Build one body publisher, reads out of SFTP
     * Loops, calls method on the subscriber (HTTP interface, gets more data!) */
    final HttpRequest post;
    try {
      post =
          HttpRequest.newBuilder(URI.create(String.format("%s/loki/api/v1/push", lokiUrl)))
              .POST(BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
              .header("Content-Type", "application/json")
              .build();
    } catch (final Exception e) {
      e.printStackTrace();
      error.labels(vidarrId).set(1);
      return;
    }
    writeTime.labels(vidarrId).set(1);
    try (final var timer = writeLatency.start(vidarrId)) {
      final var response = CLIENT.send(post, BodyHandlers.ofString());
      final var success = response.statusCode() / 100 == 2;
      if (success) {
        buffer.clear();
        error.labels(vidarrId).set(0);
      } else {
        try (final var sc = new Scanner(response.body())) {
          sc.useDelimiter("\\A");
          if (sc.hasNext()) {
            final var message = sc.next();
            if (message.contains("ignored")) {
              buffer.clear();
              error.labels(vidarrId).set(0);
              return;
            }
            System.err.println(message);
          }
        }
        error.labels(vidarrId).set(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      error.labels(vidarrId).set(1);
    }
  }
}
