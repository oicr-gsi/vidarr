package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.concurrent.TimeUnit;

/**
 * A provisioner where input and state is serialised and deserialised using Jackson
 *
 * @param <M> the type of metadata (information from the submitter)
 * @param <S> the type of stored state
 */
public abstract class BaseJsonInputProvisioner<M, S> implements InputProvisioner {
  private class JsonWorkMonitor<T> implements WorkMonitor<T, S> {

    private final WorkMonitor<T, JsonNode> original;

    private JsonWorkMonitor(WorkMonitor<T, JsonNode> original) {
      this.original = original;
    }

    @Override
    public void complete(T result) {
      original.complete(result);
    }

    @Override
    public JsonNode debugInfo() {
      return original.debugInfo();
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

    public void storeRecoveryInformation(S state) {
      original.storeRecoveryInformation(mapper.valueToTree(state));
    }

    @Override
    public void updateState(Status status) {
      original.updateState(status);
    }
  }

  private final ObjectMapper mapper;
  private final Class<M> metadataClass;
  private final Class<S> stateClass;

  /**
   * Construct a new input provisioner with data mapping
   *
   * @param mapper the Jackson object mapper configured to serialise the state, data, and metadata
   * @param stateClass the class object for the state
   * @param metadataClass the class object for the metadata (from the submitter)
   */
  protected BaseJsonInputProvisioner(
      ObjectMapper mapper, Class<S> stateClass, Class<M> metadataClass) {
    this.stateClass = stateClass;
    this.mapper = mapper;
    this.metadataClass = metadataClass;
  }

  @Override
  public final JsonNode provision(
      WorkflowLanguage language, String id, String path, WorkMonitor<JsonNode, JsonNode> monitor) {
    try {
      return mapper.valueToTree(
          provisionRegistered(language, id, path, new JsonWorkMonitor<>(monitor)));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
      return NullNode.getInstance();
    }
  }

  /**
   * Begin provisioning out a new output
   *
   * @see #provisionExternal(WorkflowLanguage, JsonNode, WorkMonitor)
   */
  protected abstract S provisionExternal(
      WorkflowLanguage language, M metadata, WorkMonitor<JsonNode, S> monitor);

  @Override
  public final JsonNode provisionExternal(
      WorkflowLanguage language, JsonNode metadata, WorkMonitor<JsonNode, JsonNode> monitor) {
    try {
      return mapper.valueToTree(
          provisionExternal(
              language,
              mapper.treeToValue(metadata, metadataClass),
              new JsonWorkMonitor<>(monitor)));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
      return NullNode.getInstance();
    }
  }

  public abstract S provisionRegistered(
      WorkflowLanguage language, String id, String path, WorkMonitor<JsonNode, S> monitor);

  /**
   * Restart a provisioning process from state saved in the database
   *
   * @see #recover(JsonNode, WorkMonitor)
   */
  protected abstract void recover(S state, WorkMonitor<JsonNode, S> monitor);

  @Override
  public final void recover(JsonNode state, WorkMonitor<JsonNode, JsonNode> monitor) {
    try {
      recover(mapper.treeToValue(state, stateClass), new JsonWorkMonitor<>(monitor));
    } catch (Exception e) {
      monitor.permanentFailure(e.toString());
    }
  }
}
