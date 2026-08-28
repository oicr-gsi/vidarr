package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationTestDoubles.response;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * A response code this step does not recognise must fail the operation
 *
 * <p>Throwing instead leaves the exception on an HTTP callback thread that nothing observes, so the
 * operation is never resolved and the workflow run waits forever.
 *
 * <p>The failure also has to be reported as the right kind. A response that only a change of
 * configuration could fix is permanent, and retrying it just delays the report by the whole retry
 * budget; everything else is worth another attempt.
 */
public class OperationStepHandleHttpStatusTest {

  private RecordingFlow<TestState, HttpResponse<String>> run(HttpResponse<String> response) {
    final var flow = new RecordingFlow<TestState, HttpResponse<String>>();
    new OperationStepHandleHttpStatus<String>()
        .run(response, new TestOperation(), new TestTransactionManager(), flow);
    return flow;
  }

  @Test
  public void anyRedirectIsAPermanentErrorEvenWithoutALocationHeader() {
    final var flow = run(response(301, Map.of()));
    assertEquals(0, flow.results().size());
    assertEquals(List.of(), flow.errors());
    assertEquals(1, flow.permanentErrors().size());
    assertTrue(flow.permanentErrors().get(0), flow.permanentErrors().get(0).contains("301"));
  }

  @Test
  public void aRedirectReportsWhereItPointed() {
    final var flow =
        run(response(302, Map.of("location", List.of("http://elsewhere.example.com"))));
    assertEquals(1, flow.permanentErrors().size());
    assertTrue(
        flow.permanentErrors().get(0),
        flow.permanentErrors().get(0).contains("http://elsewhere.example.com"));
  }

  @Test
  public void allSuccessCodesContinueWithTheSameResponse() {
    for (final int status : new int[] {200, 201, 202, 204}) {
      final var response = response(status, Map.of());
      final var flow = run(response);
      assertEquals("status " + status, 1, flow.results().size());
      assertEquals("status " + status, List.of(), flow.failures());
      assertSame("status " + status, response, flow.results().get(0));
    }
  }

  /**
   * 502, 503 and 429 are what a busy Cromwell or a proxy in front of it actually returns, and 400
   * and 404 are what one that has not caught up with a workflow yet returns. None of them were
   * handled before, so each one hung the run; all of them are worth reattempting.
   */
  @Test
  public void transientFailureCodesErrorRatherThanThrow() {
    for (final int status : new int[] {400, 404, 409, 429, 500, 502, 503, 504}) {
      final var flow = run(response(status, Map.of()));
      assertEquals("status " + status, 0, flow.results().size());
      assertEquals("status " + status, List.of(), flow.permanentErrors());
      assertEquals("status " + status, 1, flow.errors().size());
      assertTrue("status " + status, flow.errors().get(0).contains(Integer.toString(status)));
    }
  }

  /**
   * Credentials come from plugin configuration, so they will be exactly as wrong on the next
   * attempt. Spending the retry budget on them only delays telling somebody to fix the config.
   */
  @Test
  public void anAuthenticationFailureIsPermanent() {
    for (final int status : new int[] {401, 403}) {
      final var flow = run(response(status, Map.of()));
      assertEquals("status " + status, 0, flow.results().size());
      assertEquals("status " + status, List.of(), flow.errors());
      assertEquals("status " + status, 1, flow.permanentErrors().size());
      assertTrue(
          "status " + status, flow.permanentErrors().get(0).contains(Integer.toString(status)));
      assertTrue("status " + status, flow.permanentErrors().get(0).contains("credentials"));
    }
  }

  /**
   * HttpClient resolves interim responses itself, so a 1xx means the server is not speaking HTTP
   * properly. It still must not throw, and repeating the request cannot help.
   */
  @Test
  public void anInterimResponseIsAPermanentError() {
    for (final int status : new int[] {100, 101}) {
      final var flow = run(response(status, Map.of()));
      assertEquals("status " + status, 0, flow.results().size());
      assertEquals("status " + status, List.of(), flow.errors());
      assertEquals("status " + status, 1, flow.permanentErrors().size());
      assertTrue("status " + status, flow.permanentErrors().get(0).contains("final response"));
    }
  }
}
