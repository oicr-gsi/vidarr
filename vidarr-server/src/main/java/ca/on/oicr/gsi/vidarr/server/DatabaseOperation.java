package ca.on.oicr.gsi.vidarr.server;

import static ca.on.oicr.gsi.vidarr.server.jooq.Tables.*;

import ca.on.oicr.gsi.vidarr.core.ActiveOperation;
import ca.on.oicr.gsi.vidarr.core.OperationStatus;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;

public class DatabaseOperation implements ActiveOperation<DSLContext> {
  public static Optional<DatabaseOperation> create(
      DSLContext dsl, int workflowId, String type, JsonNode recoveryState) {
    return dsl.insertInto(ACTIVE_OPERATION)
        .set(ACTIVE_OPERATION.TYPE, type)
        .set(ACTIVE_OPERATION.STATUS, OperationStatus.INITIALIZING)
        .set(ACTIVE_OPERATION.WORKFLOW_RUN_ID, workflowId)
        .set(ACTIVE_OPERATION.RECOVERY_STATE, recoveryState)
        .returningResult(ACTIVE_OPERATION.ID)
        .fetchOptional()
        .map(
            id ->
                new DatabaseOperation(
                    id.value1(), recoveryState, OperationStatus.INITIALIZING, ""));
  }

  public static DatabaseOperation recover(Record record) {
    return new DatabaseOperation(
        record.get(ACTIVE_OPERATION.ID),
        record.get(ACTIVE_OPERATION.RECOVERY_STATE),
        record.get(ACTIVE_OPERATION.STATUS),
        record.get(ACTIVE_OPERATION.TYPE));
  }

  private final int id;
  private JsonNode recoveryState;
  private OperationStatus status;
  private String type;

  private DatabaseOperation(int id, JsonNode recoveryState, OperationStatus status, String type) {
    this.id = id;
    this.recoveryState = recoveryState;
    this.status = status;
    this.type = type;
  }

  @Override
  public void log(System.Logger.Level level, String message) {
    System.err.printf("%s: Operation %d: %s\n", level, id, message);
  }

  @Override
  public JsonNode recoveryState() {
    return recoveryState;
  }

  @Override
  public void recoveryState(JsonNode state, DSLContext transaction) {
    recoveryState = state;
    updateField(ACTIVE_OPERATION.RECOVERY_STATE, state, transaction);
  }

  @Override
  public OperationStatus status() {
    return status;
  }

  @Override
  public void status(OperationStatus status, DSLContext transaction) {
    this.status = status;
    updateField(ACTIVE_OPERATION.STATUS, status, transaction);
  }

  @Override
  public String type() {
    return type;
  }

  @Override
  public void type(String type, DSLContext transaction) {
    this.type = type;
    updateField(ACTIVE_OPERATION.TYPE, type, transaction);
  }

  private <T> void updateField(Field<T> field, T value, DSLContext transaction) {
    transaction
        .update(ACTIVE_OPERATION)
        .set(field, value)
        .where(ACTIVE_OPERATION.ID.eq(id))
        .execute();
  }
}
