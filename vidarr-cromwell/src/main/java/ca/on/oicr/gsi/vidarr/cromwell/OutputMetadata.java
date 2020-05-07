package ca.on.oicr.gsi.vidarr.cromwell;

/** The input data required from the caller to provision out files */
public final class OutputMetadata {
  private String outputDirectory;

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setOutputDirectory(String outputDirectory) {
    this.outputDirectory = outputDirectory;
  }
}
