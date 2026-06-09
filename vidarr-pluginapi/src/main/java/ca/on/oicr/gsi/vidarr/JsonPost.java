package ca.on.oicr.gsi.vidarr;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import java.io.IOException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.exc.MismatchedInputException;
import tools.jackson.databind.json.JsonMapper;

/**
 * An HTTP handler that accepts a deserialized JSON value as the HTTP body from a <code>POST</code>
 * request
 *
 * @param <T> the type of the bdy
 */
public interface JsonPost<T> {

  /**
   * Create an HTTP handler that parses the body of the HTTP request and delegates it to the handler
   * provided.
   *
   * @param mapper the Jackson object mapper to use
   * @param clazz the type of the body
   * @param handler the handler to delegate
   * @return an HTTP handler that will parse the request
   * @param <T> the request body type
   */
  static <T> HttpHandler parse(JsonMapper mapper, Class<T> clazz, JsonPost<T> handler) {
    return exchange ->
        exchange
            .getRequestReceiver()
            .receiveFullBytes(
                (e, data) -> {
                  try {
                    handler.handleRequest(exchange, mapper.readValue(data, clazz));
                  } catch (IOException | MismatchedInputException exception) {
                    exception.printStackTrace();
                    e.setStatusCode(StatusCodes.BAD_REQUEST);
                    e.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    if (exception.getMessage().contains("No content to map due to end-of-input")) {
                      e.getResponseSender().send("Expected content but request body was empty");
                    } else {
                      e.getResponseSender().send(exception.getMessage());
                    }
                  }
                });
  }

  /**
   * Create an HTTP handler that parses the body of the HTTP request and delegates it to the handler
   * provided.
   *
   * @param mapper the Jackson object mapper to use
   * @param type the type of the body
   * @param handler the handler to delegate
   * @return an HTTP handler that will parse the request
   * @param <T> the request body type
   */
  static <T> HttpHandler parse(JsonMapper mapper, TypeReference<T> type, JsonPost<T> handler) {
    return exchange ->
        exchange
            .getRequestReceiver()
            .receiveFullBytes(
                (e, data) -> {
                  try {
                    handler.handleRequest(exchange, mapper.readValue(data, type));
                  } catch (IOException exception) {
                    exception.printStackTrace();
                    e.setStatusCode(StatusCodes.BAD_REQUEST);
                    e.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                    e.getResponseSender().send(exception.getMessage());
                  }
                });
  }

  /**
   * Handle and HTTP request
   *
   * @param exchange the HTTP exchange, with the body already removed
   * @param body the parsed HTTP body
   * @throws IOException for when those writes fail
   */
  void handleRequest(HttpServerExchange exchange, T body) throws IOException;
}
