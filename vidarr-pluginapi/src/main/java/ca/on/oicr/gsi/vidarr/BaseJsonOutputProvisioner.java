package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.concurrent.TimeUnit;

/**
 * A provisioner where input and state is serialised and deserialised using Jackson
 *
 * @param <M> the type of metadata (information from the submitter)
 * @param <S> the type of stored state
 * @param <F> the type of stored state during preflight
 */
public abstract class BaseJsonOutputProvisioner<M, S, F> implements OutputProvisioner {
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

    public void storeRecoveryInformation(U state) {
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
  private final Class<F> preflightStateClass;

  /**
   * Construct a new output provisioner with data mapping
   *
   * @param mapper the Jackson object mapper configured to serialise the state, data, and metadata
   * @param stateClass the class object for the state
   * @param preflightStateClass the class object for the preflight state
   * @param metadataClass the class object for the metadata (from the submitter)
   */
  protected BaseJsonOutputProvisioner(
      ObjectMapper mapper,
      Class<S> stateClass,
      Class<F> preflightStateClass,
      Class<M> metadataClass) {
    this.stateClass = stateClass;
    this.mapper = mapper;
    this.preflightStateClass = preflightStateClass;
    this.metadataClass = metadataClass;
  }
  /**
   * Check that the metadata provided by the submitter is valid.
   *
   * @see #preflightCheck(JsonNode, WorkMonitor)
   */
  protected abstract F preflightCheck(M metadata, WorkMonitor<Boolean, S> monitor);

  @Override
  public final JsonNode preflightCheck(JsonNode metadata, WorkMonitor<Boolean, JsonNode> monitor) {
    try {
      return mapper.valueToTree(
          preflightCheck(
              mapper.treeToValue(metadata, metadataClass), new JsonWorkMonitor<>(monitor)));
    } catch (JsonProcessingException e) {
      monitor.permanentFailure(e.toString());
      return NullNode.getInstance();
    }
  }

  protected abstract void preflightRecover(F state, WorkMonitor<Boolean, S> monitor);

  @Override
  public final void preflightRecover(JsonNode state, WorkMonitor<Boolean, JsonNode> monitor) {
    try {
      preflightRecover(
          mapper.treeToValue(state, preflightStateClass), new JsonWorkMonitor<>(monitor));
    } catch (JsonProcessingException e) {
      monitor.permanentFailure(e.toString());
    }
  }

  /**
   * Begin provisioning out a new output
   *
   * @see OutputProvisioner#provision(String, String, JsonNode, WorkMonitor)
   */
  protected abstract S provision(
      String workflowId, String data, M metadata, WorkMonitor<Result, S> monitor);

  @Override
  public final JsonNode provision(
      String workflowRunId, String data, JsonNode metadata, WorkMonitor<Result, JsonNode> monitor) {
    try {
      return mapper.valueToTree(
          provision(
              workflowRunId,
              data,
              mapper.treeToValue(metadata, metadataClass),
              new JsonWorkMonitor<>(monitor)));
    } catch (JsonProcessingException e) {
      monitor.permanentFailure(e.toString());
      return NullNode.getInstance();
    }
  }

  /**
   * Restart a provisioning process from state saved in the database
   *
   * @see #recover(JsonNode, WorkMonitor)
   */
  protected abstract void recover(S state, WorkMonitor<Result, S> monitor);

  @Override
  public final void recover(JsonNode state, WorkMonitor<Result, JsonNode> monitor) {
    try {
      recover(mapper.treeToValue(state, stateClass), new JsonWorkMonitor<>(monitor));
    } catch (JsonProcessingException e) {
      monitor.permanentFailure(e.toString());
    }
  }
}
