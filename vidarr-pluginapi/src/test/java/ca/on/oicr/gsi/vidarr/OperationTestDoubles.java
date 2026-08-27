package ca.on.oicr.gsi.vidarr;

import java.net.URI;
import java.net.http.HttpClient.Version;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.net.ssl.SSLSession;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;

/** Minimal stand-ins for the objects an {@link OperationStep} is handed when it runs */
final class OperationTestDoubles {

  /** The state a step carries; steps require a record, but the contents are irrelevant here. */
  record TestState(String value) {}

  /** Records what a step did instead of passing control to a real successor. */
  static final class RecordingFlow<Result> implements OperationControlFlow<TestState, Result> {

    private int cancels;
    private final List<String> errors = new java.util.ArrayList<>();
    private final List<Result> results = new java.util.ArrayList<>();
    private Runnable onNext = () -> {};

    int cancels() {
      return cancels;
    }

    @Override
    public void cancel() {
      cancels++;
    }

    @Override
    public void error(String error) {
      errors.add(error);
    }

    List<String> errors() {
      return errors;
    }

    /** Make the successor of this step misbehave, as a buggy plugin or phase transition would. */
    void failOnNext(Runnable failure) {
      onNext = failure;
    }

    @Override
    public void next(Result result) {
      results.add(result);
      onNext.run();
    }

    List<Result> results() {
      return results;
    }

    @Override
    public JsonNode serializeNestedState(TestState state) {
      return JsonNodeFactory.instance.textNode(state.value());
    }
  }

  /** An operation that keeps its updates in memory. */
  static final class TestOperation implements ActiveOperation<Void> {

    private String error;
    private boolean live = true;
    private final List<String> logs = new java.util.ArrayList<>();
    private JsonNode recoveryState = JsonNodeFactory.instance.nullNode();
    private OperationStatus status = OperationStatus.INITIALIZING;
    private String type;

    @Override
    public void debugInfo(JsonNode info, Void transaction) {
      // Not interesting for these tests.
    }

    @Override
    public void error(String reason, Void transaction) {
      error = reason;
    }

    String error() {
      return error;
    }

    @Override
    public boolean isLive() {
      return live;
    }

    void kill() {
      live = false;
    }

    @Override
    public void log(System.Logger.Level level, String message) {
      logs.add(message);
    }

    List<String> logs() {
      return logs;
    }

    @Override
    public JsonNode recoveryState() {
      return recoveryState;
    }

    @Override
    public void recoveryState(JsonNode state, Void transaction) {
      recoveryState = state;
    }

    @Override
    public OperationStatus status() {
      return status;
    }

    @Override
    public void status(OperationStatus status, Void transaction) {
      this.status = status;
    }

    @Override
    public String type() {
      return type;
    }

    @Override
    public void type(String type, Void transaction) {
      this.type = type;
    }
  }

  /** A transaction manager that runs everything inline so tests stay deterministic. */
  static final class TestTransactionManager implements ActiveOperation.TransactionManager<Void> {

    @Override
    public void inTransaction(Consumer<Void> transaction) {
      transaction.accept(null);
    }

    @Override
    public void scheduleTask(Runnable task) {
      task.run();
    }

    @Override
    public void scheduleTask(long delay, TimeUnit units, Runnable task) {
      task.run();
    }
  }

  /**
   * Build an HTTP response with the given status code
   *
   * @param statusCode the status to report
   * @param headers the response headers, which control whether a redirect names a location
   */
  static HttpResponse<String> response(int statusCode, Map<String, List<String>> headers) {
    return new HttpResponse<>() {
      @Override
      public String body() {
        return "";
      }

      @Override
      public HttpHeaders headers() {
        return HttpHeaders.of(headers, (a, b) -> true);
      }

      @Override
      public Optional<HttpResponse<String>> previousResponse() {
        return Optional.empty();
      }

      @Override
      public HttpRequest request() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Optional<SSLSession> sslSession() {
        return Optional.empty();
      }

      @Override
      public int statusCode() {
        return statusCode;
      }

      @Override
      public URI uri() {
        return URI.create("http://cromwell.example.com:8000/api/workflows/v1/abc/outputs");
      }

      @Override
      public Version version() {
        return Version.HTTP_1_1;
      }
    };
  }

  private OperationTestDoubles() {}
}
