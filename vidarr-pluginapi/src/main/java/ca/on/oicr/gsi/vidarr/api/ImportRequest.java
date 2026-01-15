package ca.on.oicr.gsi.vidarr.api;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o){
    if (this == o) return true;
    if (null == o || this.getClass() != o.getClass()) return false;

    ImportRequest other = (ImportRequest) o;
    return super.equals(other)
        && Objects.equals(this.outputPath, other.outputPath)
        && Objects.equals(this.outputProvisionerName, other.outputProvisionerName);
  }

  @Override
  public int hashCode(){
    return Objects.hash(super.hashCode(), outputPath, outputProvisionerName);
  }
}
