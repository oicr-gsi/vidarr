package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationTestDoubles.response;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
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
 */
public class OperationStepHandleHttpStatusTest {

  private RecordingFlow<HttpResponse<String>> run(HttpResponse<String> response) {
    final var flow = new RecordingFlow<HttpResponse<String>>();
    new OperationStepHandleHttpStatus<String>()
        .run(response, new TestOperation(), new TestTransactionManager(), flow);
    return flow;
  }

  @Test
  public void anyRedirectIsAnErrorEvenWithoutALocationHeader() {
    final var flow = run(response(301, Map.of()));
    assertEquals(0, flow.results().size());
    assertEquals(1, flow.errors().size());
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("301"));
  }

  @Test
  public void aRedirectReportsWhereItPointed() {
    final var flow = run(response(302, Map.of("location", List.of("http://elsewhere.example.com"))));
    assertEquals(1, flow.errors().size());
    assertTrue(
        flow.errors().get(0), flow.errors().get(0).contains("http://elsewhere.example.com"));
  }

  @Test
  public void allSuccessCodesContinue() {
    for (final int status : new int[] {200, 201, 202, 204}) {
      final var flow = run(response(status, Map.of()));
      assertEquals("status " + status, 1, flow.results().size());
      assertEquals("status " + status, 0, flow.errors().size());
    }
  }

  /**
   * 502, 503 and 429 are what a busy Cromwell or a proxy in front of it actually returns, and 401
   * and 403 are what a misconfigured one returns. None of them were handled before, so each one
   * hung the run.
   */
  @Test
  public void allFailureCodesErrorRatherThanThrow() {
    for (final int status : new int[] {400, 401, 403, 404, 429, 500, 502, 503, 504}) {
      final var flow = run(response(status, Map.of()));
      assertEquals("status " + status, 0, flow.results().size());
      assertEquals("status " + status, 1, flow.errors().size());
      assertTrue(
          "status " + status, flow.errors().get(0).contains(Integer.toString(status)));
    }
  }
}
