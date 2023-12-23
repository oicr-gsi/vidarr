package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.concurrent.TimeUnit;

/**
 * A runtime provisioner where input and state is serialised and deserialised using Jackson
 *
 * @param <S> the type of stored state
 */
public abstract class BaseJsonRuntimeProvisioner<S> implements RuntimeProvisioner {
  private class JsonWorkMonitor<T, U> implements WorkMonitor<T, U> {
    private final WorkMonitor<T, JsonNode> original;

    private JsonWorkMonitor(WorkMonitor<T, JsonNode> original) {
      this.original = original;
    }

    @Override
    public void complete(T result) {
      original.complete(result);
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

  private final ObjectMapper mapper;
  private final Class<S> stateClass;

  /**
   * Construct a new output provisioner with data mapping
   *
   * @param mapper the Jackson object mapper configured to serialise the state, data, and metadata
   * @param stateClass the class object for the state
   */
  protected BaseJsonRuntimeProvisioner(ObjectMapper mapper, Class<S> stateClass) {
    this.stateClass = stateClass;
    this.mapper = mapper;
  }
  /**
   * Begin provisioning out a new output
   *
   * @see OutputProvisioner#provision(String, String, JsonNode, WorkMonitor)
   */
  protected abstract S provision(
      String workflowId, String data, WorkMonitor<OutputProvisioner.Result, S> monitor);

  @Override
  public final JsonNode provision(
      String workflowRunId, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor) {
    try {
      return mapper.valueToTree(provision(workflowRunId, new JsonWorkMonitor<>(monitor)));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
      return NullNode.getInstance();
    }
  }

  /**
   * Restart a provisioning process from state saved in the database
   *
   * @see #recover(JsonNode, WorkMonitor)
   */
  protected abstract void recover(S state, WorkMonitor<OutputProvisioner.Result, S> monitor);

  @Override
  public final void recover(
      JsonNode state, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor) {
    try {
      recover(mapper.treeToValue(state, stateClass), new JsonWorkMonitor<>(monitor));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
    }
  }

  /**
   * Restart a failed provisioning process from state saved in the database
   *
   * @see #recover(JsonNode, WorkMonitor)
   */
  protected abstract void retry(S state, WorkMonitor<OutputProvisioner.Result, S> monitor);

  @Override
  public final void retry(JsonNode state, WorkMonitor<OutputProvisioner.Result, JsonNode> monitor) {
    try {
      retry(mapper.treeToValue(state, stateClass), new JsonWorkMonitor<>(monitor));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
    }
  }
}
