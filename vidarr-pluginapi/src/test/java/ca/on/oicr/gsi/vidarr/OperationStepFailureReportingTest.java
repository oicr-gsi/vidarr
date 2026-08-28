package ca.on.oicr.gsi.vidarr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import java.lang.System.Logger.Level;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Every step that calls plugin code must report what went wrong, not just that something did
 *
 * <p>These steps all catch the exception their plugin function threw and turn it into an operation
 * error. Reporting {@code getMessage()} meant that the failures which carry no message — a {@link
 * NullPointerException} above all — recorded a null error, so the operation failed with nothing to
 * say why. Each step is covered here because each one catches separately.
 */
public class OperationStepFailureReportingTest {

  /** The steps that run a plugin-supplied function, each rigged to have that function throw. */
  private static Map<String, OperationStep<String, ?>> stepsThatCallPluginCode(
      RuntimeException failure) {
    final var steps = new LinkedHashMap<String, OperationStep<String, ?>>();
    steps.put(
        "debugInfo",
        OperationStep.debugInfo(
            value -> {
              throw failure;
            }));
    steps.put(
        "log",
        OperationStep.log(
            Level.INFO,
            value -> {
              throw failure;
            }));
    steps.put(
        "mapping",
        OperationStep.mapping(
            value -> {
              throw failure;
            }));
    steps.put(
        "require",
        OperationStep.require(
            value -> {
              throw failure;
            },
            "the value was not acceptable"));
    steps.put(
        "status",
        OperationStep.status(
            value -> {
              throw failure;
            }));
    return steps;
  }

  private RecordingFlow<TestState, Object> run(OperationStep<String, ?> step) {
    final var flow = new RecordingFlow<TestState, Object>();
    @SuppressWarnings("unchecked")
    final var typed = (OperationStep<String, Object>) step;
    typed.run("value", new TestOperation(), new TestTransactionManager(), flow);
    return flow;
  }

  @Test
  public void everyStepReportsTheMessageItWasGiven() {
    for (final var step :
        stepsThatCallPluginCode(new IllegalStateException("the reference genome is missing"))
            .entrySet()) {
      final var flow = run(step.getValue());
      assertEquals(step.getKey(), 0, flow.results().size());
      assertEquals(step.getKey(), 1, flow.errors().size());
      assertEquals(step.getKey(), "the reference genome is missing", flow.errors().get(0));
    }
  }

  /** The regression: a failure with no message must not become a null error. */
  @Test
  public void everyStepReportsAFailureThatHasNoMessage() {
    for (final var step : stepsThatCallPluginCode(new NullPointerException()).entrySet()) {
      final var flow = run(step.getValue());
      assertEquals(step.getKey(), 0, flow.results().size());
      assertEquals(step.getKey(), 1, flow.errors().size());
      assertNotNull(step.getKey(), flow.errors().get(0));
      assertTrue(step.getKey(), flow.errors().get(0).contains("NullPointerException"));
    }
  }

  /** Loading the state is plugin code too, and it is the first thing an operation does. */
  @Test
  public void aLoaderThatThrowsWithoutAMessageReportsSomething() {
    final var flow = new RecordingFlow<TestState, String>();
    OperationAction.<TestState, String>load(
            TestState.class,
            state -> {
              throw new NullPointerException();
            })
        .launch(new TestState("value"))
        .launch(new TestOperation(), new TestTransactionManager(), flow);
    assertEquals(0, flow.results().size());
    assertEquals(1, flow.errors().size());
    assertNotNull(flow.errors().get(0));
    assertTrue(flow.errors().get(0), flow.errors().get(0).contains("NullPointerException"));
  }

  /** A step whose plugin code behaves passes the value along untouched. */
  @Test
  public void aWorkingStepIsUnaffected() {
    final var flow = new RecordingFlow<TestState, String>();
    OperationStep.<String, String>mapping(value -> value + " transformed")
        .run("value", new TestOperation(), new TestTransactionManager(), flow);
    assertEquals(0, flow.errors().size());
    assertEquals("value transformed", flow.results().get(0));
  }
}
