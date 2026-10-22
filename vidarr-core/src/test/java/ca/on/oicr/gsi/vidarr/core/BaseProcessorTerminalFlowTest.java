package ca.on.oicr.gsi.vidarr.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.vidarr.ActiveOperation;
import ca.on.oicr.gsi.vidarr.OperationStatus;
import ca.on.oicr.gsi.vidarr.core.BaseProcessor.TerminalHandler;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.JsonNodeFactory;

/**
 * What the framework does when an operation fails after it has already been resolved
 *
 * <p>The rest of an operation runs inside callbacks, so the wrapping up — serializing the result,
 * starting the next phase — can fail after the operation has been reported as finished. There is no
 * operation left to fail at that point, but the workflow run cannot continue either, so the
 * operation is failed anyway to stop the run from waiting for a phase that will never start. That
 * has to happen exactly once, because every transition to {@code FAILED} releases the run's
 * consumable resources again.
 */
public class BaseProcessorTerminalFlowTest {

  /** The state a flow carries; only the fact that it is a record matters here. */
  public record TestState(String value) {}

  /** An operation that records every transition, so a repeated report is visible. */
  private static final class RecordingOperation implements ActiveOperation<Void> {

    private final List<String> errors = new ArrayList<>();
    private final List<String> logs = new ArrayList<>();
    private JsonNode recoveryState = JsonNodeFactory.instance.nullNode();
    private OperationStatus status = OperationStatus.INITIALIZING;
    private final List<OperationStatus> transitions = new ArrayList<>();
    private String type;

    @Override
    public void debugInfo(JsonNode info, Void transaction) {
      // Not interesting for these tests.
    }

    @Override
    public void error(String reason, Void transaction) {
      errors.add(reason);
    }

    List<String> errors() {
      return errors;
    }

    @Override
    public boolean isLive() {
      return true;
    }

    @Override
    public void log(System.Logger.Level level, String message) {
      logs.add(message);
    }

    List<String> logs() {
      return logs;
    }

    @Override
    public JsonNode recoveryState() {
      return recoveryState;
    }

    @Override
    public void recoveryState(JsonNode state, Void transaction) {
      recoveryState = state;
    }

    @Override
    public OperationStatus status() {
      return status;
    }

    @Override
    public void status(OperationStatus status, Void transaction) {
      this.status = status;
      transitions.add(status);
    }

    /** Every status this operation was moved to, in order. */
    List<OperationStatus> transitions() {
      return transitions;
    }

    @Override
    public String type() {
      return type;
    }

    @Override
    public void type(String type, Void transaction) {
      this.type = type;
    }
  }

  /** A phase handler that counts what it was told, because being told twice is the bug. */
  private static final class RecordingHandler implements TerminalHandler<String> {

    private int failed;
    private int succeeded;

    @Override
    public void failed() {
      failed++;
    }

    int failures() {
      return failed;
    }

    @Override
    public JsonNode serialize(String output) {
      return JsonNodeFactory.instance.textNode(output);
    }

    @Override
    public void succeeded(String output) {
      succeeded++;
    }

    int successes() {
      return succeeded;
    }
  }

  /** The smallest processor that can hand out a terminal control flow. */
  private static final class TestProcessor
      extends BaseProcessor<ActiveWorkflow<RecordingOperation, Void>, RecordingOperation, Void> {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    TestProcessor(ScheduledExecutorService executor) {
      super(executor);
    }

    @Override
    public void inTransaction(Consumer<Void> transaction) {
      transaction.accept(null);
    }

    @Override
    protected JsonMapper mapper() {
      return MAPPER;
    }

    @Override
    public Optional<FileMetadata> pathForId(String id) {
      return Optional.empty();
    }
  }

  private ScheduledExecutorService executor;
  private TestProcessor processor;

  @Before
  public void setUp() {
    executor = Executors.newScheduledThreadPool(1);
    processor = new TestProcessor(executor);
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  /**
   * The first failure resolves the operation and tells the phase. A failure after that still has to
   * mark the operation failed, or nothing fails the run.
   */
  @Test
  public void aFailureAfterTheOperationIsResolvedStillFailsTheOperation() {
    final var operation = new RecordingOperation();
    final var handler = new RecordingHandler();
    final var flow = processor.<TestState, String>createNext(operation, handler);
    flow.error("the phase could not be started");
    flow.error("and the result could not be serialized either");
    assertEquals(
        List.of(OperationStatus.FAILED, OperationStatus.FAILED), operation.transitions());
    assertEquals(
        List.of("the phase could not be started", "and the result could not be serialized either"),
        operation.errors());
  }

  /**
   * The phase handler is deliberately left out of the second report: both handlers that track
   * anything share one countdown between {@code succeeded()} and {@code failed()}, so telling it
   * twice would resolve the phase before this operation's siblings had finished.
   */
  @Test
  public void aFailureAfterTheOperationIsResolvedDoesNotTellThePhaseAgain() {
    final var handler = new RecordingHandler();
    final var flow = processor.<TestState, String>createNext(new RecordingOperation(), handler);
    flow.error("the phase could not be started");
    flow.error("and the result could not be serialized either");
    flow.error("nor could the next one");
    assertEquals(1, handler.failures());
    assertEquals(0, handler.successes());
  }

  /**
   * The regression this guards: each {@code FAILED} is a fresh phase transition to the store, which
   * releases the workflow run's consumable resources again, so only the first one is reported.
   */
  @Test
  public void anOperationIsNotFailedRepeatedlyAfterItIsResolved() {
    final var operation = new RecordingOperation();
    final var flow =
        processor.<TestState, String>createNext(operation, new RecordingHandler());
    flow.error("the phase could not be started");
    flow.error("and the result could not be serialized either");
    for (var attempt = 0; attempt < 5; attempt++) {
      flow.error("something else went wrong while unwinding");
    }
    assertEquals(2, operation.transitions().size());
    assertEquals(2, operation.errors().size());
    assertEquals(2, operation.logs().size());
  }

  /** Succeeding twice is a bug in the framework rather than a run that needs to be failed. */
  @Test
  public void aSuccessAfterTheOperationIsResolvedIsStillRejected() {
    final var flow =
        processor.<TestState, String>createNext(new RecordingOperation(), new RecordingHandler());
    flow.next("the first result");
    assertThrows(IllegalStateException.class, () -> flow.next("a second result"));
  }

  /**
   * Schedule a task that throws and return everything that reached stderr while it ran
   *
   * @param schedule the overload of {@code scheduleTask} under test
   * @param message the message the task throws, which is also what the wait watches for
   */
  private String stderrFromAFailing(Consumer<Runnable> schedule, String message) throws Exception {
    final var captured = new ByteArrayOutputStream();
    final var original = System.err;
    System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
    try {
      final var ran = new CountDownLatch(1);
      schedule.accept(
          () -> {
            ran.countDown();
            throw new IllegalStateException(message);
          });
      assertTrue("the task should have run", ran.await(10, TimeUnit.SECONDS));
      /* The trace is printed on the executor thread just after the task returns, so wait for it
       * rather than assume it has already happened. Bounded so that a lost report fails rather
       * than hangs, and generously enough that a loaded machine does not fail spuriously. */
      for (var attempt = 0;
          attempt < 500 && !captured.toString(StandardCharsets.UTF_8).contains(message);
          attempt++) {
        Thread.sleep(10);
      }
      return captured.toString(StandardCharsets.UTF_8);
    } finally {
      System.setErr(original);
    }
  }

  private static void assertReported(String stderr, String message) {
    assertTrue(
        "the failure should have been printed, but stderr held: " + stderr,
        stderr.contains("java.lang.IllegalStateException: " + message));
    assertTrue(
        "a stack trace should accompany it, but stderr held: " + stderr, stderr.contains("\tat "));
  }

  /**
   * A task that belongs to an operation is guarded by the step that scheduled it, but nothing
   * guards the rest, and the executor's future is never observed. The printed stack trace is all
   * that stands between such a failure and no trace at all.
   */
  @Test
  public void aFailureInAScheduledTaskIsReported() throws Exception {
    assertReported(
        stderrFromAFailing(processor::scheduleTask, "nobody is watching this"),
        "nobody is watching this");
  }

  /**
   * A delayed task is wrapped separately, so it needs saying separately, and it is the likelier of
   * the two to go unnoticed: work that resumes on a timer — a poll, a retry, a sleep — has no
   * caller left anywhere to notice that it never came back.
   */
  @Test
  public void aFailureInADelayedTaskIsReported() throws Exception {
    assertReported(
        stderrFromAFailing(
            task -> processor.scheduleTask(1, TimeUnit.MILLISECONDS, task),
            "nobody is watching this either"),
        "nobody is watching this either");
  }

  /** The same wrapping applies to a delayed task, and it must not stop the task from running. */
  @Test
  public void aDelayedTaskStillRuns() throws Exception {
    final var ran = new CountDownLatch(1);
    processor.scheduleTask(1, TimeUnit.MILLISECONDS, ran::countDown);
    assertTrue("the delayed task should have run", ran.await(30, TimeUnit.SECONDS));
  }
}
