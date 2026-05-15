package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import java.net.http.HttpResponse;

/**
 * Some external HTTP services may return error codes when overloaded, but reattempting the same
 * operation later could succeed. This errors gracefuly on 400, 404 and 500 response codes, which
 * allows for reattempting the requests when chained with e.g. {@link
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
    int status = input.statusCode();
    switch (status) {
      case 200:
      case 201:
        next.next(input);
        break;

      case 301:
        String locationFromRedirect = input.headers().map().get("location").get(0);
        throw new RuntimeException(
            String.format(
                "HTTP request to %s returned 301 redirect URL %s. The HTTP request URL cannot be adjusted on the fly.",
                input.uri(), locationFromRedirect));
      case 400:
      case 404:
      case 500:
        next.error(
            String.format(
                "HTTP request to %s failed with status: %d", input.uri(), input.statusCode()));
        break;
      default:
        throw new RuntimeException(
            String.format(
                "HTTP request to %s received unhandled HTTP input status %s",
                input.uri(), input.statusCode()));
    }
  }
}
