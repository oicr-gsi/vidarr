package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import java.net.http.HttpResponse;

/**
 * Some external HTTP services may return error codes when overloaded, but reattempting the same
 * operation later could succeed. This errors gracefully on any non-2xx response code, which allows
 * for reattempting the requests when chained with e.g. {@link
 * OperationStatefulStepRepeatUntilSuccess}
 *
 * <p>A response that repeating the request cannot improve on is reported through {@link
 * OperationControlFlow#permanentError(String)} instead, so that it fails the operation now rather
 * than after the whole retry budget has been spent. That covers responses that depend only on
 * configuration: a redirect, since the request URL is built from plugin configuration, and a
 * refusal to authenticate or authorise. Everything else stays retryable, deliberately: a 404 from a
 * workflow engine often means "not registered yet" rather than "never", and a 400 can come from a
 * request built out of a result that has not settled.
 */
public final class OperationStepHandleHttpStatus<Body>
    extends OperationStep<HttpResponse<Body>, HttpResponse<Body>> {

  public OperationStepHandleHttpStatus() {
    super();
  }

  @Override
  public <State extends Record, TX> void run(
      HttpResponse<Body> input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, HttpResponse<Body>> next) {
    final int status = input.statusCode();
    if (status / 100 == 2) {
      next.next(input);
    } else if (status / 100 == 1) {
      /* HttpClient deals with interim responses itself and only ever hands over a final status, so
       * a 1xx means the server is not speaking HTTP properly. Repeating the request will not teach
       * it to. */
      next.permanentError(
          String.format(
              "HTTP request to %s returned interim status %d rather than a final response.",
              input.uri(), status));
    } else if (status / 100 == 3) {
      // A redirect cannot be followed because the request URL is built from plugin configuration
      // and cannot be adjusted on the fly, so it will point somewhere just as unusable next time.
      next.permanentError(
          String.format(
              "HTTP request to %s returned %d redirect to %s. The HTTP request URL cannot be"
                  + " adjusted on the fly.",
              input.uri(), status, input.headers().firstValue("location").orElse("(unspecified)")));
    } else if (status == 401 || status == 403) {
      // Credentials are configuration too, so they will be no better when a retry runs.
      next.permanentError(
          String.format(
              "HTTP request to %s was refused with status: %d. Check the credentials configured for"
                  + " this plugin.",
              input.uri(), status));
    } else {
      next.error(String.format("HTTP request to %s failed with status: %d", input.uri(), status));
    }
  }
}
