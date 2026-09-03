package ca.on.oicr.gsi.vidarr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.OperationTestDoubles.RecordingFlow;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestOperation;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestState;
import ca.on.oicr.gsi.vidarr.OperationTestDoubles.TestTransactionManager;
import io.prometheus.client.Counter;
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
 * say why. Every step that catches is covered here, because each one catches separately: the plain
 * steps, the stateful steps that do the same job with the state in hand, and the two odd ones out,
 * {@link OperationStep#monitorWhen(io.prometheus.client.Counter, java.util.function.Predicate,
 * String...)} and {@link OperationAction#reload(OperationAction.Loader)}.
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

  /**
   * The stateful twins of those steps, which do the same job with the state in hand
   *
   * <p>Each is expressed as a whole action because a stateful step wraps the action before it
   * rather than running on its own.
   */
  private static Map<String, OperationAction<TestState, TestState, ?>>
      statefulStepsThatCallPluginCode(RuntimeException failure) {
    final var source = OperationAction.load(TestState.class, TestState::value);
    final var steps = new LinkedHashMap<String, OperationAction<TestState, TestState, ?>>();
    steps.put(
        "stateful debugInfo",
        source.then(
            OperationStatefulStep.debugInfo(
                (state, value) -> {
                  throw failure;
                })));
    steps.put(
        "stateful log",
        source.then(
            OperationStatefulStep.log(
                Level.INFO,
                (state, value) -> {
                  throw failure;
                })));
    steps.put(
        "stateful mapping",
        source.then(
            OperationStatefulStep.mapping(
                (state, value) -> {
                  throw failure;
                })));
    steps.put(
        "stateful require",
        source.then(
            OperationStatefulStep.require(
                (state, value) -> {
                  throw failure;
                },
                "the value was not acceptable")));
    steps.put(
        "stateful status",
        source.then(
            OperationStatefulStep.status(
                (state, value) -> {
                  throw failure;
                })));
    steps.put(
        "monitor",
        source.then(
            OperationStep.monitorWhen(
                Counter.build().name("test_operations").help("Test counter").create(),
                value -> {
                  throw failure;
                })));
    steps.put(
        "reload",
        source.reload(
            state -> {
              throw failure;
            }));
    return steps;
  }

  private RecordingFlow<TestState, Object> run(OperationAction<TestState, TestState, ?> action) {
    final var flow = new RecordingFlow<TestState, Object>();
    @SuppressWarnings("unchecked")
    final var typed = (OperationAction<TestState, TestState, Object>) action;
    typed
        .launch(new TestState("value"))
        .launch(new TestOperation(), new TestTransactionManager(), flow);
    return flow;
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

  @Test
  public void everyStatefulStepReportsTheMessageItWasGiven() {
    for (final var step :
        statefulStepsThatCallPluginCode(
                new IllegalStateException("the reference genome is missing"))
            .entrySet()) {
      final var flow = run(step.getValue());
      assertEquals(step.getKey(), 0, flow.results().size());
      assertEquals(step.getKey(), 1, flow.errors().size());
      assertEquals(step.getKey(), "the reference genome is missing", flow.errors().get(0));
    }
  }

  @Test
  public void everyStatefulStepReportsAFailureThatHasNoMessage() {
    for (final var step :
        statefulStepsThatCallPluginCode(new NullPointerException()).entrySet()) {
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
