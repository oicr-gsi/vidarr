package ca.on.oicr.gsi.vidarr.cromwell;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

/** The response from Cromwell when requesting the status of a workflow */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowMetadataResponse {
  private Map<String, List<CromwellCall>> calls;
  private String id;
  private String status;
  private String workflowRoot;
  private List<CromwellFailure> failures = List.of();

  public JsonNode debugInfo() {
    final var debugInfo = CromwellWorkflowEngine.MAPPER.createObjectNode();
    debugInfo.put("type", "cromwell");
    debugInfo.put("cromwellId", id);
    debugInfo.put("cromwellStatus", status);
    debugInfo.put("cromwellRoot", workflowRoot);
    debugInfo.putPOJO("cromwellFailures", failures);

    // `calls` might be empty if workflow run is not failed
    if (null != calls && calls.size() != 0) {
      final var cromwellCalls = debugInfo.putArray("cromwellCalls");
      calls.forEach(
          (task, calls) ->
              calls.forEach(
                  call -> {
                    final var callNode = cromwellCalls.addObject();
                    callNode.put("task", task);
                    callNode.put("attempt", call.getAttempt());
                    callNode.put("backend", call.getBackend());
                    callNode.put("jobId", call.getJobId());
                    callNode.put("returnCode", call.getReturnCode());
                    callNode.put("shardIndex", call.getShardIndex());
                    callNode.put("stderr", call.getStderr());
                    callNode.put("stdout", call.getStdout());
                    callNode.put("executionStatus", call.getExecutionStatus());
                    callNode.putPOJO("failures", call.getFailures());
                  }));
    }
    return debugInfo;
  }

  public Map<String, List<CromwellCall>> getCalls() {
    return calls;
  }

  public List<CromwellFailure> getFailures() {
    return failures;
  }

  public String getId() {
    return id;
  }

  public String getStatus() {
    return status;
  }

  public String getWorkflowRoot() {
    return workflowRoot;
  }

  public void setCalls(Map<String, List<CromwellCall>> calls) {
    this.calls = calls;
  }

  public void setFailures(List<CromwellFailure> failures) {
    this.failures = failures;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public void setWorkflowRoot(String workflowRoot) {
    this.workflowRoot = workflowRoot;
  }
}
