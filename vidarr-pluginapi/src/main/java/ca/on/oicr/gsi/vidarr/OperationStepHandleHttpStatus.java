package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import java.net.http.HttpResponse;

/**
 * Some external HTTP services may return error codes when overloaded, but reattempting the same
 * operation later could succeed. This errors gracefully on any non-2xx response code, which allows
 * for reattempting the requests when chained with e.g. {@link
 * OperationStatefulStepRepeatUntilSuccess}
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
    } else if (status / 100 == 3) {
      // A redirect cannot be followed because the request URL is built from plugin configuration
      // and cannot be adjusted on the fly.
      next.error(
          String.format(
              "HTTP request to %s returned %d redirect to %s. The HTTP request URL cannot be"
                  + " adjusted on the fly.",
              input.uri(), status, input.headers().firstValue("location").orElse("(unspecified)")));
    } else {
      next.error(
          String.format("HTTP request to %s failed with status: %d", input.uri(), status));
    }
  }
}
