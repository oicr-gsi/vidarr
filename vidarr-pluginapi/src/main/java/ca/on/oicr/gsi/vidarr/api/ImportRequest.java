package ca.on.oicr.gsi.vidarr.api;

public class ImportRequest extends UnloadedData {
  private String outputProvisionerName;
  private String outputPath;

  public String getOutputPath() {
    return outputPath;
  }

  public String getOutputProvisionerName() {
    return outputProvisionerName;
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
}
