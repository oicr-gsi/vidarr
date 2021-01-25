package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class SingleShotWorkflow implements ActiveWorkflow<SingleShotOperation, Void> {
  private final JsonNode arguments;
  private JsonNode cleanup;
  private final JsonNode engineArguments;
  private Set<ExternalId> externalIds;
  private boolean extraInputIdsHandled;
  private final CompletableFuture<Boolean> future = new CompletableFuture<>();
  private final Set<ExternalId> inputIds;
  private boolean isPreflightOkay = true;
  private final JsonNode metadata;
  private Phase phase = Phase.INITIALIZING;
  private final String prefix;
  private ObjectNode realInput;
  private final OutputProvisioningHandler<Void> resultHandler;

  SingleShotWorkflow(
      String prefix,
      JsonNode arguments,
      JsonNode engineArguments,
      JsonNode metadata,
      Stream<ExternalId> inputIds,
      OutputProvisioningHandler<Void> resultHandler) {
    this.prefix = prefix;
    this.arguments = arguments;
    this.engineArguments = engineArguments;
    this.metadata = metadata;
    this.inputIds = inputIds.collect(Collectors.toSet());
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
  public void cleanup(JsonNode cleanupState, Void transaction) {
    cleanup = cleanupState;
  }

  @Override
  public JsonNode engineArguments() {
    return engineArguments;
  }

  @Override
  public boolean extraInputIdsHandled() {
    return extraInputIdsHandled;
  }

  @Override
  public void extraInputIdsHandled(boolean extraInputIdsHandled, Void transaction) {
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
  public Set<ExternalId> inputIds() {
    return inputIds;
  }

  @Override
  public boolean isPreflightOkay() {
    return isPreflightOkay;
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
  public List<SingleShotOperation> phase(
      Phase phase, List<Pair<String, JsonNode>> operationInitialStates, Void transaction) {
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
          .map(i -> new SingleShotOperation(i.second(), this))
          .collect(Collectors.toList());
    }
  }

  @Override
  public void preflightFailed(Void transaction) {
    System.err.printf("%s: [%s] Preflight failed%n", prefix, Instant.now());
    isPreflightOkay = false;
  }

  @Override
  public void provisionFile(
      Set<? extends ExternalId> ids,
      String storagePath,
      String md5,
      String metatype,
      long fileSize,
      Map<String, String> labels,
      Void transaction) {
    resultHandler.provisionFile(ids, storagePath, md5, metatype, fileSize, labels, transaction);
  }

  @Override
  public void provisionUrl(
      Set<? extends ExternalId> ids, String url, Map<String, String> labels, Void transaction) {
    resultHandler.provisionUrl(ids, url, labels, transaction);
  }

  @Override
  public ObjectNode realInput() {
    return realInput;
  }

  @Override
  public void realInput(ObjectNode realInput, Void transaction) {
    this.realInput = realInput;
  }

  @Override
  public Set<ExternalId> requestedExternalIds() {
    return externalIds;
  }

  @Override
  public void requestedExternalIds(Set<ExternalId> requiredExternalIds, Void transaction) {
    externalIds = requiredExternalIds;
  }

  @Override
  public void runUrl(String workflowRunUrl, Void transaction) {}

  @Override
  public void succeeded(Void transaction) {
    future.complete(true);
  }
}
