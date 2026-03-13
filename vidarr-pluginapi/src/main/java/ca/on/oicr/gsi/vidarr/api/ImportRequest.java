package ca.on.oicr.gsi.vidarr.api;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ImportRequest {
  private ProvenanceWorkflowRun<ExternalMultiVersionKey> workflowRun;
  private UnloadedWorkflowVersion workflowVersion;
  private UnloadedWorkflow workflow;
  private String outputProvisionerName;
  private String outputPath;

  /**
   * Do preliminary setup from a WorkflowDeclaration.
   * Does not set: workflow version's `workflow` (ie the full text of the shell script,
   * WDL workflow, etc.), 'accessory files' even if a workflow version has them,
   * anything about the workflow run.
   *
   * @param declaration
   * @return partially set up ImportRequest
   */
  public static ImportRequest fromDeclaration(WorkflowDeclaration declaration){
    ImportRequest request = new ImportRequest();

    UnloadedWorkflow adaptedWorkflow = new UnloadedWorkflow();
    adaptedWorkflow.setName(declaration.getName());
    adaptedWorkflow.setLabels(declaration.getLabels());
    request.setWorkflow(adaptedWorkflow);

    UnloadedWorkflowVersion adaptedVersion = new UnloadedWorkflowVersion();
    adaptedVersion.setName(declaration.getName());
    adaptedVersion.setVersion(declaration.getVersion());
    adaptedVersion.setAccessoryFiles(Map.of());
    adaptedVersion.setLanguage(declaration.getLanguage());
    adaptedVersion.setOutputs(declaration.getMetadata());
    adaptedVersion.setParameters(declaration.getParameters());

    request.setWorkflowVersion(adaptedVersion);

    request.setWorkflowRun(new ProvenanceWorkflowRun<>());

    return request;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public String getOutputProvisionerName() {
    return outputProvisionerName;
  }

  public UnloadedWorkflow getWorkflow() {
    return workflow;
  }

  public ProvenanceWorkflowRun<ExternalMultiVersionKey> getWorkflowRun() {
    return workflowRun;
  }

  public UnloadedWorkflowVersion getWorkflowVersion() {
    return workflowVersion;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public void setOutputProvisionerName(String outputProvisionerName) {
    this.outputProvisionerName = outputProvisionerName;
  }

  public ReprovisionOutRequest reprovision(String hash){
    ReprovisionOutRequest ret = new ReprovisionOutRequest();
    ret.setOutputPath(outputPath);
    ret.setOutputProvisionerName(outputProvisionerName);
    ret.setWorkflowRunHashId(hash);
    return ret;
  }

  public UnloadedData load(){
    UnloadedData ret = new UnloadedData();
    ret.setWorkflows(List.of(workflow));
    ret.setWorkflowVersions(List.of(workflowVersion));
    ret.setWorkflowRuns(List.of(workflowRun));
    return ret;
  }

  public void check() throws Exception {
    boolean ok = true;
    StringBuilder error = new StringBuilder("Malformed import request: ");
    if (null == outputPath || outputPath.isBlank()){
      error.append("outputPath is empty. ");
      ok = false;
    }
    if (null == outputProvisionerName || outputProvisionerName.isBlank()){
      error.append("outputProvisionerName is empty. ");
      ok = false;
    }
    if (null == workflow){
      error.append("workflow is empty. ");
      ok = false;
    }
    if (null == workflowRun) {
      error.append("workflowRun is empty. ");
      ok = false;
    }
    if (null == workflowVersion){
      error.append("workflowVersion is empty. ");
      ok = false;
    }
    if (!ok) throw new Exception(error.toString());
  }

  @Override
  public boolean equals(Object o){
    if (this == o) return true;
    if (null == o || this.getClass() != o.getClass()) return false;

    ImportRequest other = (ImportRequest) o;
    return Objects.equals(this.outputPath, other.outputPath)
        && Objects.equals(this.outputProvisionerName, other.outputProvisionerName)
        && Objects.equals(this.workflowRun, other.workflowRun)
        && Objects.equals(this.workflow, other.workflow)
        && Objects.equals(this.workflowVersion, other.workflowVersion);
  }

  @Override
  public int hashCode(){
    return Objects.hash(outputPath, outputProvisionerName, workflowRun, workflowVersion, workflow);
  }

  public void setWorkflow(UnloadedWorkflow workflow) {
    this.workflow = workflow;
  }

  public void setWorkflowRun(
      ProvenanceWorkflowRun<ExternalMultiVersionKey> workflowRun) {
    this.workflowRun = workflowRun;
  }

  public void setWorkflowVersion(UnloadedWorkflowVersion workflowVersion) {
    this.workflowVersion = workflowVersion;
  }
}
