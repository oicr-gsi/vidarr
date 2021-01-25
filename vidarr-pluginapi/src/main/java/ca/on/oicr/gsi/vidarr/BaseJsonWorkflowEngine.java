package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.concurrent.TimeUnit;

/**
 * A workflow engine where the state is serialised using Jackson
 *
 * @param <S> the Java type for the state
 * @param <C> the Java type for the cleanup
 * @param <D> the Java type for the cleanup state
 */
public abstract class BaseJsonWorkflowEngine<S, C, D> implements WorkflowEngine {
  private abstract class BaseJsonWorkMonitor<I, O, U> implements WorkMonitor<I, U> {
    protected final WorkMonitor<O, JsonNode> original;

    BaseJsonWorkMonitor(WorkMonitor<O, JsonNode> original) {
      this.original = original;
    }

    @Override
    public void log(System.Logger.Level level, String message) {
      original.log(level, message);
    }

    @Override
    public void permanentFailure(String reason) {
      original.permanentFailure(reason);
    }

    @Override
    public void scheduleTask(long delay, TimeUnit units, Runnable task) {
      original.scheduleTask(delay, units, task);
    }

    @Override
    public void scheduleTask(Runnable task) {
      original.scheduleTask(task);
    }

    @Override
    public void storeDebugInfo(JsonNode information) {
      original.storeDebugInfo(information);
    }

    public void storeRecoveryInformation(U state) {
      original.storeRecoveryInformation(mapper.valueToTree(state));
    }

    @Override
    public void updateState(Status status) {
      original.updateState(status);
    }
  }

  private class JsonCleanupWorkMonitor extends BaseJsonWorkMonitor<Void, Void, D> {

    public JsonCleanupWorkMonitor(WorkMonitor<Void, JsonNode> monitor) {
      super(monitor);
    }

    @Override
    public void complete(Void result) {
      original.complete(result);
    }
  }

  private class JsonWorkMonitor extends BaseJsonWorkMonitor<Result<C>, Result<JsonNode>, S> {

    JsonWorkMonitor(WorkMonitor<Result<JsonNode>, JsonNode> original) {
      super(original);
    }

    @Override
    public void complete(Result<C> result) {
      original.complete(
          new Result<>(
              result.output(),
              result.workflowRunUrl(),
              result.cleanupState().map(mapper::valueToTree)));
    }
  }

  private final Class<C> cleanupClass;
  private final Class<D> cleanupStateClass;
  private final ObjectMapper mapper;
  private final Class<S> stateClass;

  /**
   * Construct a workflow engine with a state mapper
   *
   * @param mapper the Jackson object mapper configured to serialise the state
   * @param stateClass the class object for the state
   * @param cleanupClass the class object for the cleanup
   * @param cleanupStateClass the class object for the cleanup state
   */
  protected BaseJsonWorkflowEngine(
      ObjectMapper mapper, Class<S> stateClass, Class<C> cleanupClass, Class<D> cleanupStateClass) {
    this.mapper = mapper;
    this.stateClass = stateClass;
    this.cleanupClass = cleanupClass;
    this.cleanupStateClass = cleanupStateClass;
  }

  /**
   * Clean up the output of a workflow (i.e., delete its on-disk output) after provisioning has been
   * completed.
   *
   * @see #cleanup(JsonNode, WorkMonitor)
   */
  protected abstract D cleanup(C cleanupState, WorkMonitor<Void, D> monitor);

  @Override
  public final JsonNode cleanup(JsonNode cleanupState, WorkMonitor<Void, JsonNode> monitor) {
    try {
      return mapper.valueToTree(
          cleanup(
              mapper.treeToValue(cleanupState, cleanupClass), new JsonCleanupWorkMonitor(monitor)));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
      return JsonNodeFactory.instance.nullNode();
    }
  }

  /**
   * Restart a running process from state saved in the database
   *
   * @see #recover(JsonNode, WorkMonitor)
   */
  protected abstract void recover(S state, WorkMonitor<Result<C>, S> monitor);

  @Override
  public final void recover(JsonNode state, WorkMonitor<Result<JsonNode>, JsonNode> monitor) {
    try {
      recover(mapper.treeToValue(state, stateClass), new JsonWorkMonitor(monitor));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
    }
  }

  protected abstract void recoverCleanup(D state, WorkMonitor<Void, D> monitor);

  @Override
  public void recoverCleanup(JsonNode state, WorkMonitor<Void, JsonNode> monitor) {
    try {
      recoverCleanup(
          mapper.treeToValue(state, cleanupStateClass), new JsonCleanupWorkMonitor(monitor));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
    }
  }

  @Override
  public final JsonNode run(
      WorkflowLanguage workflowLanguage,
      String workflow,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters,
      WorkMonitor<Result<JsonNode>, JsonNode> monitor) {
    return mapper.valueToTree(
        runWorkflow(
            workflowLanguage,
            workflow,
            vidarrId,
            workflowParameters,
            engineParameters,
            new JsonWorkMonitor(monitor)));
  }
  /**
   * Start a new workflow
   *
   * @see WorkflowEngine#run(WorkflowLanguage, String, String, ObjectNode, JsonNode, WorkMonitor)
   */
  protected abstract S runWorkflow(
      WorkflowLanguage workflowLanguage,
      String workflow,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters,
      WorkMonitor<Result<C>, S> monitor);
}
