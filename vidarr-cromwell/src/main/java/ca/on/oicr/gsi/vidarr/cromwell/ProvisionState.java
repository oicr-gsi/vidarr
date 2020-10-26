package ca.on.oicr.gsi.vidarr.cromwell;

/** The current provisioning state, to be recorded in the database */
public final class ProvisionState {
  private String cromwellId;
  private String fileName;
  private String metaType;
  private String outputPrefix;
  private String vidarrId;

  public String getCromwellId() {
    return cromwellId;
  }

  public String getFileName() {
    return fileName;
  }

  public String getMetaType() {
    return metaType;
  }

  public String getOutputPrefix() {
    return outputPrefix;
  }

  public String getVidarrId() {
    return vidarrId;
  }

  public void setCromwellId(String cromwellId) {
    this.cromwellId = cromwellId;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public void setMetaType(String metaType) {
    this.metaType = metaType;
  }

  public void setOutputPrefix(String outputPrefix) {
    this.outputPrefix = outputPrefix;
  }

  public void setVidarrId(String vidarrId) {
    this.vidarrId = vidarrId;
  }
}
