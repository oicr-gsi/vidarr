package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.core.ActiveWorkflow;
import ca.on.oicr.gsi.vidarr.core.ExternalId;
import ca.on.oicr.gsi.vidarr.core.ExternalKey;
import ca.on.oicr.gsi.vidarr.core.Phase;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import java.util.stream.Collectors;

final class SingleShotWorkflow
    implements ActiveWorkflow<SingleShotOperation, SingleShotTransaction> {
  private final JsonNode arguments;
  private JsonNode cleanup;
  private final ObjectNode engineArguments;
  private List<ExternalId> externalIds;
  private boolean extraInputIdsHandled;
  private Set<? extends ExternalId> inputIds;
  private boolean isPreflightOkay = true;
  private final JsonNode metadata;
  private final Supplier<ObjectNode> supplier;
  private Phase phase = Phase.INITIALIZING;
  private ObjectNode realInput;
  private String runUrl;
  private final Semaphore semaphore = new Semaphore(1);

  SingleShotWorkflow(
      JsonNode arguments,
      ObjectNode engineArguments,
      JsonNode metadata,
      Supplier<ObjectNode> supplier) {
    this.arguments = arguments;
    this.engineArguments = engineArguments;
    this.metadata = metadata;
    this.supplier = supplier;
    semaphore.acquireUninterruptibly();
  }

  @Override
  public JsonNode arguments() {
    return arguments;
  }

  public void await() {
    semaphore.acquireUninterruptibly();
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
    semaphore.release();
  }

  @Override
  public String id() {
    return "test";
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

  private boolean success;

  public boolean isSuccessful() {
    return success;
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
      Phase phase, List<JsonNode> operationInitialStates, SingleShotTransaction transaction) {
    System.err.println("Transitioning to phase: " + phase);
    this.phase = phase;
    if (phase == Phase.FAILED) {
      semaphore.release();
      return List.of();
    } else {
      System.err.println("Operations to complete: " + operationInitialStates.size());
      return operationInitialStates.stream()
          .map(i -> new SingleShotOperation(i, this))
          .collect(Collectors.toList());
    }
  }

  @Override
  public void preflightFailed(SingleShotTransaction transaction) {
    System.err.println("Preflight failed");
    isPreflightOkay = false;
  }

  @Override
  public void provisionFile(
      Set<? extends ExternalId> ids,
      String storagePath,
      String md5,
      String metatype,
      Map<String, String> labels,
      SingleShotTransaction transaction) {
    System.err.println("Provisioning out file");
    final var node = supplier.get();
    node.put("type", "file");
    node.put("storagePath", storagePath);
    node.put("md5", md5);
    node.put("metatype", metatype);
    final var outputIds = node.putArray("ids");
    for (final var id : ids) {
      final var outputId = outputIds.addObject();
      outputId.put("id", id.getId());
      outputId.put("provider", id.getProvider());
    }
    final var outputLabels = node.putObject("labels");
    for (final var label : labels.entrySet()) {
      outputLabels.put(label.getKey(), label.getValue());
    }
  }

  @Override
  public void provisionUrl(
      Set<? extends ExternalId> ids,
      String url,
      Map<String, String> labels,
      SingleShotTransaction transaction) {
    System.err.println("Provisioning out URL");
    final var node = supplier.get();
    node.put("type", "url");
    node.put("url", url);
    final var outputIds = node.putArray("ids");
    for (final var id : ids) {
      final var outputId = outputIds.addObject();
      outputId.put("id", id.getId());
      outputId.put("provider", id.getProvider());
    }
    final var outputLabels = node.putObject("labels");
    for (final var label : labels.entrySet()) {
      outputLabels.put(label.getKey(), label.getValue());
    }
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
  public String runUrl() {
    return runUrl;
  }

  @Override
  public void runUrl(String workflowRunUrl, SingleShotTransaction transaction) {
    runUrl = workflowRunUrl;
  }

  @Override
  public void succeeded(SingleShotTransaction transaction) {
    success = true;
    semaphore.release();
  }
}
