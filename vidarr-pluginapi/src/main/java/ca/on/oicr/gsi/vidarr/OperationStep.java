package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import com.fasterxml.jackson.databind.JsonNode;
import io.prometheus.client.Counter;
import java.lang.ProcessBuilder.Redirect;
import java.lang.System.Logger.Level;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Operation steps modify the values being carried by an {@link OperationAction} without changing
 * the state
 *
 * <p>It's reasonable to consider every step as a function that takes an input value and provides an
 * output value. Unlike a normal function, steps can be asynchronous and write information to
 * Vidarr's database.
 *
 * @param <Input> the parameter type
 * @param <Output> the return type
 */
public abstract sealed class OperationStep<Input, Output>
    permits OperationStepCompletableFuture,
        OperationStepDebugInfo,
        OperationStepHandleHttpStatus,
        OperationStepLog,
        OperationStepMapping,
        OperationStepMonitor,
        OperationStepRequire,
        OperationStepSleep,
        OperationStepStatus,
        OperationStepThen {

  /**
   * A simple mapping function
   *
   * <p>This is analogous to {@link java.util.function.Function}, but it can throw an exception
   * which will be caught and logged to Vidarr's database.
   *
   * @param <Input> the parameter type
   * @param <Output> the return type
   */
  public interface Transformer<Input, Output> {

    /**
     * Call the function with an argument
     *
     * @param input the argument to use
     * @return the transformed value
     * @throws Exception any exceptions will be caught and redirected through Vidarr's logging and
     *     operation framework
     */
    Output transform(Input input) throws Exception;
  }

  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  /**
   * Write new debugging information for this value
   *
   * @param fetch the transformation to produce debugging information to write to the database.
   *     Vidarr imposes no schema on this data; it is up to the client to interpret it
   * @param <Value> the type of the input value
   * @return a step to perform write this debugging information
   */
  public static <Value> OperationStep<Value, Value> debugInfo(Transformer<Value, JsonNode> fetch) {
    return new OperationStepDebugInfo<>(fetch);
  }

  /**
   * Perform an asynchronous step using a {@link CompletableFuture} from Java's thread pool
   * infrastructure
   *
   * @param <Value> the type produced by the completable future
   * @return a step to create and wait for a future
   */
  public static <Value> OperationStep<CompletableFuture<Value>, Value> future() {
    return new OperationStepCompletableFuture<>();
  }

  /**
   * Perform an HTTP request and collect the output
   *
   * @param body the handler to extract the body of the HTTP request
   * @param <Body> the type of the response body
   * @return a step to perform this HTTP request
   */
  public static <Body> OperationStep<HttpRequest, HttpResponse<Body>> http(BodyHandler<Body> body) {
    return OperationStep.<HttpRequest, CompletableFuture<HttpResponse<Body>>>mapping(
            httpRequest -> HTTP_CLIENT.sendAsync(httpRequest, body))
        .then(future());
  }

  public static <Value> OperationStep<Value, Value> handleHttpResponseCode() {
    return new OperationStepHandleHttpStatus<>();
  }

  /**
   * Decode an HTTP response body as JSON
   *
   * @param <Body> the type of the HTTP response JSON
   * @return a step that performs the extraction
   */
  public static <Body> OperationStep<HttpResponse<Supplier<Body>>, Body> getJson() {
    return OperationStep.<Supplier<Body>>getResponseBody().then(mapping(Supplier::get));
  }

  /**
   * Get an HTTP response body
   *
   * @param <Body> the type of the HTTP response body
   * @return a step that performs the extraction
   */
  public static <Body> OperationStep<HttpResponse<Body>, Body> getResponseBody() {
    return mapping(HttpResponse::body);
  }

  /**
   * Check if an HTTP response was successful
   *
   * @param response the HTTP response to check
   * @return true if the code is 2xx; false otherwise
   */
  public static boolean isHttpOk(HttpResponse<?> response) {
    return response.statusCode() / 100 == 2;
  }

  /**
   * Check if an HTTP response was not successful. The inverse of {@link #isHttpOk}
   *
   * @param response the HTTP response to check
   * @return false if the code is 2xx; true otherwise
   */
  public static boolean isHttpNotOk(HttpResponse<?> response) {
    return !isHttpOk(response);
  }

  /**
   * Log information about the current value
   *
   * @param level the logging level
   * @param message a transformer to generate the log message
   * @param <Value> the type of the input value
   * @return a step to write this log message
   */
  public static <Value> OperationStep<Value, Value> log(
      Level level, Transformer<Value, String> message) {
    return new OperationStepLog<>(level, message);
  }

  /**
   * Change the input value using a function
   *
   * @param transformer the function to apply to the input value
   * @param <Input> the type of the input
   * @param <Output> the type of the output
   * @return a step to call this function
   */
  public static <Input, Output> OperationStep<Input, Output> mapping(
      Transformer<Input, Output> transformer) {
    return new OperationStepMapping<>(transformer);
  }

  /**
   * Increment a Prometheus counter
   *
   * @param counter the counter to increment
   * @param labels the label values for the counter
   * @param <Value> the type of the input and (unchanged) output
   * @return a step to increment this counter
   */
  public static <Value> OperationStep<Value, Value> monitor(Counter counter, String... labels) {
    return monitorWhen(counter, x -> true, labels);
  }

  /**
   * Increment a Prometheus counter if a condition
   *
   * @param counter the counter to increment
   * @param success a test to determine if the counter should be incremented
   * @param labels the label values for the counter
   * @param <Value> the type of the input and (unchanged) output
   * @return a step to increment this counter
   */
  public static <Value> OperationStep<Value, Value> monitorWhen(
      Counter counter, Predicate<Value> success, String... labels) {
    return new OperationStepMonitor<>(counter, success, labels);
  }

  /**
   * Abort the operation if a condition is not met
   *
   * @param success the test to determine if the sequence should continue (true) or go into an error
   *     state (false)
   * @param failureMessage the message to display when a failure occurs
   * @param <Value> the type of the input and (unchanged) output
   * @return a step to check this condition
   */
  public static <Value> OperationStep<Value, Value> require(
      Predicate<Value> success, String failureMessage) {
    return new OperationStepRequire<>(success, failureMessage);
  }

  /**
   * Check that an HTTP response returned a 2xx code and extract the body
   *
   * @param <Body> the type of the HTTP response body
   * @return a step that performs this check and extraction
   */
  public static <Body> OperationStep<HttpResponse<Body>, Body> requireHttpSuccess() {
    return mapping(
        response -> {
          if (isHttpOk(response)) {
            return response.body();
          } else {
            throw new RuntimeException(
                String.format(
                    "HTTP request to %s failed with status: %d",
                    response.uri(), response.statusCode()));
          }
        });
  }

  /**
   * Check that an HTTP response with a JSON body returned a 2xx code and decode the body
   *
   * @param <Body> the type of the HTTP response JSON
   * @return a step that performs this check and extraction
   */
  public static <Body> OperationStep<HttpResponse<Supplier<Body>>, Body> requireJsonSuccess() {
    return OperationStep.<Supplier<Body>>requireHttpSuccess().then(mapping(Supplier::get));
  }

  /**
   * Require an optional value is not empty
   *
   * @param <Value> the value inside the optional
   * @return a step that performs this check
   */
  public static <Value> OperationStep<Optional<Value>, Value> requirePresent() {
    return mapping(Optional::orElseThrow);
  }

  /**
   * Wait before executing the next steep
   *
   * @param duration the amount of time to wait
   * @param <Value> the type of the input and (unchanged) output
   * @return a step that waits
   */
  public static <Value> OperationStep<Value, Value> sleep(Duration duration) {
    return new OperationStepSleep<>(duration);
  }

  /**
   * Unconditionally update the status of the operation
   *
   * @param status the status to change to
   * @param <Value> the type of the input and (unchanged) output
   * @return a step that changes the status
   */
  public static <Value> OperationStep<Value, Value> status(WorkingStatus status) {
    return status(v -> status);
  }

  /**
   * Change the status of this operation based on the current value
   *
   * @param fetch a function that examines the current value and produces a corresponding status
   * @param <Value> the type of the input and (unchanged) output
   * @return a step that changes the status
   */
  public static <Value> OperationStep<Value, Value> status(
      Transformer<Value, WorkingStatus> fetch) {
    return new OperationStepStatus<>(fetch);
  }

  /**
   * Launch a program on the system <strong>running the Vidarr server</strong>
   *
   * @param output the handling of standard output that is desired
   * @param <Output> the data collected from standard output
   * @return a step that runs this process
   */
  public static <Output> OperationStep<ProcessInput, ProcessOutput<Output>> subprocess(
      ProcessOutputHandler<Output> output) {
    return mapping(
        input -> {
          final var build = new ProcessBuilder().command(input.command());
          if (input.standardInput().isPresent()) {
            build.redirectInput(Redirect.PIPE);
          }
          final var outputGenerator = output.prepare(build);
          final var process = build.start();
          if (input.standardInput().isPresent()) {
            try (final var stdin = process.getOutputStream()) {
              stdin.write(input.standardInput().get());
            }
          }
          if (input.maximumWait().isPresent()) {
            final var duration = input.maximumWait().get();
            if (!process.waitFor(duration.get(TimeUnit.SECONDS.toChronoUnit()), TimeUnit.SECONDS)) {
              process.destroy();
              throw new RuntimeException(
                  String.format("Killed process %d after timeout", process.pid()));
            }
          } else {
            process.waitFor();
          }
          return new ProcessOutput<>(
              process.exitValue(), outputGenerator.get(process.exitValue() == 0));
        });
  }

  abstract <State extends Record, TX> void run(
      Input input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Output> next);

  /**
   * Perform two operations in sequence
   *
   * <p>This provides the same functionality as {@link OperationAction#then(OperationStep)}.
   * Normally, the {@link OperationAction#then(OperationStep)} makes for more linear, readable code,
   * but this method can be useful for pre-composing utility steps.
   *
   * @param step the following step
   * @param <Value> the final output after the second step is applied
   * @return a combined step
   */
  public final <Value> OperationStep<Input, Value> then(OperationStep<Output, Value> step) {
    return new OperationStepThen<>(this, step);
  }
}
