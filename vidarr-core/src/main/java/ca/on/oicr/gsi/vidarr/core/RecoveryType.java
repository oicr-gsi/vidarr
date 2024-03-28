package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OperationAction.Launcher;
import com.fasterxml.jackson.databind.JsonNode;

public enum RecoveryType {
  RECOVER {
    @Override
    public <State extends Record, OriginalState extends Record, Value>
        Launcher<State, Value> prepare(
            OperationAction<State, OriginalState, Value> action, JsonNode state) {
      return action.recover(state);
    }
  },
  RETRY {
    @Override
    public <State extends Record, OriginalState extends Record, Value>
        Launcher<State, Value> prepare(
            OperationAction<State, OriginalState, Value> action, JsonNode state) {
      return action.retry(state);
    }
  };

  public abstract <State extends Record, OriginalState extends Record, Value>
      Launcher<State, Value> prepare(
          OperationAction<State, OriginalState, Value> action, JsonNode state);
}
