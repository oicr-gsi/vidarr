package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

final class SingleShotWorkflow
    implements ActiveWorkflow<SingleShotOperation, SingleShotTransaction> {
  private final String prefix;
  private final JsonNode arguments;
  private JsonNode cleanup;
  private final ObjectNode engineArguments;
  private List<ExternalId> externalIds;
  private boolean extraInputIdsHandled;
  private final CompletableFuture<Boolean> future = new CompletableFuture<>();
  private Set<? extends ExternalId> inputIds;
  private boolean isPreflightOkay = true;
  private final JsonNode metadata;
  private Phase phase = Phase.INITIALIZING;
  private ObjectNode realInput;
  private boolean success;
  private final OutputProvisioningHandler<SingleShotTransaction> resultHandler;

  SingleShotWorkflow(
      String prefix,
      JsonNode arguments,
      ObjectNode engineArguments,
      JsonNode metadata,
      OutputProvisioningHandler<SingleShotTransaction> resultHandler) {
    this.prefix = prefix;
    this.arguments = arguments;
    this.engineArguments = engineArguments;
    this.metadata = metadata;
    this.resultHandler = resultHandler;
  }

  @Override
  public JsonNode arguments() {
    return arguments;
  }

  public boolean await() {
    return future.join();
  }

  @Override
  public JsonNode cleanup() {
    return cleanup;
  }

  @Override
  public void cleanup(JsonNode cleanupState, SingleShotTransaction transaction) {
    cleanup = cleanupState;
  }

  @Override
  public ObjectNode engineArguments() {
    return engineArguments;
  }

  @Override
  public List<ExternalId> externalIds() {
    return externalIds;
  }

  @Override
  public void externalIds(List<ExternalId> requiredExternalIds) {
    externalIds = requiredExternalIds;
  }

  @Override
  public boolean extraInputIdsHandled() {
    return extraInputIdsHandled;
  }

  @Override
  public void extraInputIdsHandled(
      boolean extraInputIdsHandled, SingleShotTransaction transaction) {
    this.extraInputIdsHandled = extraInputIdsHandled;
  }

  public void fail() {
    System.err.printf("%s: [%s] Workflow operation failed%n", prefix, Instant.now());
    future.complete(false);
  }

  public CompletableFuture<Boolean> future() {
    return future;
  }

  @Override
  public String id() {
    return prefix;
  }

  @Override
  public Set<? extends ExternalId> inputIds() {
    return inputIds;
  }

  @Override
  public void inputIds(Set<ExternalKey> inputIds, SingleShotTransaction transaction) {
    this.inputIds = inputIds;
  }

  @Override
  public boolean isPreflightOkay() {
    return isPreflightOkay;
  }

  public boolean isSuccessful() {
    return success;
  }

  public void log(System.Logger.Level level, String message) {
    System.err.printf("%s: [%s] Operation %s: %s%n", prefix, Instant.now(), level.name(), message);
  }

  @Override
  public JsonNode metadata() {
    return metadata;
  }

  @Override
  public Phase phase() {
    return phase;
  }

  @Override
  public void provisionFile(
      Set<? extends ExternalId> ids,
      String storagePath,
      String md5,
      String metatype,
      Map<String, String> labels,
      SingleShotTransaction transaction) {
    resultHandler.provisionFile(ids, storagePath, md5, metatype, labels, transaction);
  }

  @Override
  public void provisionUrl(
      Set<? extends ExternalId> ids,
      String url,
      Map<String, String> labels,
      SingleShotTransaction transaction) {
    resultHandler.provisionUrl(ids, url, labels, transaction);
  }

  @Override
  public List<SingleShotOperation> phase(
      Phase phase, List<JsonNode> operationInitialStates, SingleShotTransaction transaction) {
    System.err.printf("%s: [%s] Transitioning to phase: %s%n", prefix, Instant.now(), phase);
    this.phase = phase;
    if (phase == Phase.FAILED) {
      future.complete(false);
      return List.of();
    } else {
      System.err.printf(
          "%s: [%s] Operations to complete: %s%n",
          prefix, Instant.now(), operationInitialStates.size());
      return operationInitialStates.stream()
          .map(i -> new SingleShotOperation(i, this))
          .collect(Collectors.toList());
    }
  }

  @Override
  public void preflightFailed(SingleShotTransaction transaction) {
    System.err.printf("%s: [%s] Preflight failed%n", prefix, Instant.now());
    isPreflightOkay = false;
  }

  @Override
  public ObjectNode realInput() {
    return realInput;
  }

  @Override
  public void realInput(ObjectNode realInput, SingleShotTransaction transaction) {
    this.realInput = realInput;
  }

  @Override
  public void runUrl(String workflowRunUrl, SingleShotTransaction transaction) {}

  @Override
  public void succeeded(SingleShotTransaction transaction) {
    success = true;
    future.complete(true);
  }
}
