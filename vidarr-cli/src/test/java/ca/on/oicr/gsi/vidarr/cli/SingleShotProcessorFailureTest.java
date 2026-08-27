package ca.on.oicr.gsi.vidarr.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.InputType;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputType;
import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.WorkflowDefinition;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.core.NoOpWorkflowEngine;
import ca.on.oicr.gsi.vidarr.core.OutputProvisioningHandler;
import ca.on.oicr.gsi.vidarr.core.RawInputProvisioner;
import ca.on.oicr.gsi.vidarr.core.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * A workflow run must always reach a conclusion
 *
 * <p>The steps that make up an operation are driven from HTTP completion callbacks and from tasks on
 * Vidarr's executor, and nothing observes the outcome of either. A bug in a step, a plugin or a
 * phase transition used to leave its exception on one of those threads with no way to see it, so the
 * run was never resolved: {@code vidarr test} printed the last thing that went well and then waited
 * forever. Every test here drives a run that fails somewhere the framework did not anticipate and
 * asserts that it finishes, quickly, with a reported reason.
 */
public class SingleShotProcessorFailureTest {

  /** The state records the stub provisioner uses; the contents do not matter. */
  public record StubPreflightState() {}

  public record StubProvisionState(String data) {}

  /** A provisioner that can be made to misbehave in the ways real ones have. */
  private static final class StubOutputProvisioner
      implements OutputProvisioner<StubPreflightState, StubProvisionState> {

    private final boolean crashWhenBuilding;

    StubOutputProvisioner(boolean crashWhenBuilding) {
      this.crashWhenBuilding = crashWhenBuilding;
    }

    @Override
    public OperationAction<?, StubProvisionState, Result> build() {
      if (crashWhenBuilding) {
        throw new IllegalStateException("provisioner is misconfigured");
      }
      return OperationAction.load(
          StubProvisionState.class,
          state -> Result.file(state.data(), "d41d8cd98f00b204e9800998ecf8427e", "md5sum", 0L,
              "text/plain"));
    }

    @Override
    public OperationAction<?, StubPreflightState, Boolean> buildPreflight() {
      return OperationAction.value(StubPreflightState.class, true);
    }

    @Override
    public boolean canProvision(OutputProvisionFormat format) {
      return format == OutputProvisionFormat.FILES;
    }

    @Override
    public void configuration(SectionRenderer sectionRenderer) {}

    @Override
    public StubPreflightState preflightCheck(tools.jackson.databind.JsonNode metadata) {
      return new StubPreflightState();
    }

    @Override
    public StubProvisionState prepareProvisionInput(
        String workflowRunId, String data, tools.jackson.databind.JsonNode metadata) {
      return new StubProvisionState(data);
    }

    @Override
    public void startup() {}

    @Override
    public String type() {
      return "stub";
    }

    @Override
    public BasicType typeFor(OutputProvisionFormat format) {
      return BasicType.object(new Pair<>("outputDirectory", BasicType.STRING));
    }
  }

  private static final JsonMapper MAPPER =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .build();
  private static final String INPUT_FILE = "/tmp/vidarr-test-input.txt";
  private static final String OUTPUT = "test.out";
  /** Long enough that a slow machine does not fail, short enough that a hang is obvious. */
  private static final long TIMEOUT_SECONDS = 30;

  private java.util.concurrent.ScheduledExecutorService executor;
  private final List<String> provisioned = new ArrayList<>();

  /**
   * The workflow takes one external file and, being a no-op engine, emits its path as the output.
   *
   * <p>A file input is what gives the run something to provision in; a run with nothing to
   * provision in never reaches the running phase.
   */
  private ObjectNode arguments() {
    final var arguments = MAPPER.createObjectNode();
    final var parameter = arguments.putObject(OUTPUT);
    parameter.put("type", "EXTERNAL");
    final var contents = parameter.putObject("contents");
    contents.put("configuration", INPUT_FILE);
    contents.putArray("externalIds").addObject().put("id", "TEST").put("provider", "TEST");
    return arguments;
  }

  private ObjectNode metadata() {
    final var metadata = MAPPER.createObjectNode();
    final var output = metadata.putObject(OUTPUT);
    output.put("type", "ALL");
    output.putArray("contents").addObject().put("outputDirectory", "/tmp");
    return metadata;
  }

  private OutputProvisioningHandler<Void> recordingHandler() {
    return new OutputProvisioningHandler<>() {
      @Override
      public void provisionFile(
          Set<? extends ExternalId> ids,
          String storagePath,
          String checksum,
          String checksumType,
          String metatype,
          long fileSize,
          Map<String, String> labels,
          Void transaction) {
        provisioned.add(storagePath);
      }

      @Override
      public void provisionUrl(
          Set<? extends ExternalId> ids, String url, Map<String, String> labels, Void transaction) {
        provisioned.add(url);
      }
    };
  }

  /**
   * Run a workflow to completion and return whether it succeeded
   *
   * @throws TimeoutException if the run never reaches a conclusion, which is the bug being guarded
   *     against
   */
  private boolean runToCompletion(OutputType outputType, OutputProvisioner<?, ?> provisioner)
      throws ExecutionException, InterruptedException, TimeoutException {
    final var run =
        new SingleShotProcessor(executor)
            .startAsync(
                "test-run",
                target(provisioner),
                workflow(outputType),
                arguments(),
                metadata(),
                MAPPER.createObjectNode(),
                recordingHandler());
    assertNotNull("the run should have been accepted", run);
    return run.future().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  @Before
  public void setUp() {
    executor = Executors.newScheduledThreadPool(2);
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  private Target target(OutputProvisioner<?, ?> outputProvisioner) {
    final var engine = new NoOpWorkflowEngine();
    final var inputProvisioner = new RawInputProvisioner();
    inputProvisioner.setFormats(Set.of(InputProvisionFormat.FILE));
    return new Target() {
      @Override
      public Stream<Pair<String, ConsumableResource>> consumableResources() {
        return Stream.empty();
      }

      @Override
      public WorkflowEngine<?, ?> engine() {
        return engine;
      }

      @Override
      public InputProvisioner<?> provisionerFor(InputProvisionFormat type) {
        return type == InputProvisionFormat.FILE ? inputProvisioner : null;
      }

      @Override
      public OutputProvisioner<?, ?> provisionerFor(OutputProvisionFormat type) {
        return outputProvisioner != null && outputProvisioner.canProvision(type)
            ? outputProvisioner
            : null;
      }

      @Override
      public Stream<RuntimeProvisioner<?>> runtimeProvisioners() {
        return Stream.empty();
      }
    };
  }

  /** A workflow whose single output is fed straight from its single parameter. */
  private WorkflowDefinition workflow(OutputType outputType) {
    return new WorkflowDefinition(
        WorkflowLanguage.WDL_1_0,
        "test",
        "",
        Map.of(),
        Stream.of(new WorkflowDefinition.Parameter(InputType.FILE, OUTPUT)),
        Stream.of(new WorkflowDefinition.Output(outputType, OUTPUT)));
  }

  /**
   * The case that was reported: the workflow ran to completion, and then provisioning out was set up
   * with output that did not match the declared type. Deciding what to provision happens inside the
   * callback that reports the workflow's success, so the resulting exception was invisible.
   */
  @Test
  public void workflowOutputOfTheWrongShapeFailsTheRun() throws Exception {
    // The workflow promises a file and a set of labels, but produces a bare string.
    assertFalse(
        runToCompletion(OutputType.FILE_WITH_LABELS, new StubOutputProvisioner(false)));
    assertEquals(List.of(), provisioned);
  }

  /** A plugin that throws while being set up must fail the run, not stall it. */
  @Test
  public void anOutputProvisionerThatCrashesWhenStartedFailsTheRun() throws Exception {
    assertFalse(runToCompletion(OutputType.FILE, new StubOutputProvisioner(true)));
    assertEquals(List.of(), provisioned);
  }

  /** Sanity check that the guards did not turn a working run into a failing one. */
  @Test
  public void aWorkingRunStillSucceeds() throws Exception {
    assertTrue(runToCompletion(OutputType.FILE, new StubOutputProvisioner(false)));
    assertEquals(List.of(INPUT_FILE), provisioned);
  }
}
