package ca.on.oicr.gsi.vidarr;

import ca.on.oicr.gsi.vidarr.ActiveOperation.TransactionManager;
import java.net.http.HttpResponse;

public final class OperationStepHandleHttpStatus<Value> extends
    OperationStep<Value, Value> {

  public OperationStepHandleHttpStatus() {
    super();
  }

  @Override
  public <State extends Record, TX> void run(
      Value input,
      ActiveOperation<TX> operation,
      TransactionManager<TX> transactionManager,
      OperationControlFlow<State, Value> next) {
    HttpResponse<?> response = (HttpResponse<?>) input;
    int status = response.statusCode();
    switch (status) {
      case 200:
      case 201:
        next.next(input);
        break;
      case 301:
        String locationFromRedirect = response.headers().map().get("location").get(0);
        throw new RuntimeException(String.format(
            "HTTP request to %s returned 301 redirect URL %s. The HTTP request URL cannot be adjusted on the fly.",
            response.uri(), locationFromRedirect));
      case 400:
      case 404:
        // Impossible to determine if this is a true 404 or Cromwell is giving bad information, but
        // this might be retryable
        next.error(String.format(
            "HTTP request to %s failed with status: %d",
            response.uri(), response.statusCode()));
      case 500:
        // This might be retryable, so error instead of throwing
        next.error(String.format(
            "HTTP request to %s failed with status: %d",
            response.uri(), response.statusCode()));
        break;
      default:
        throw new RuntimeException(
            String.format("HTTP request to %s received unhandled HTTP response status %s",
                response.uri(), response.statusCode()));
    }
  }
}