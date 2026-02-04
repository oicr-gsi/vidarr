package ca.on.oicr.gsi.vidarr.cromwell;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** The input data required from the caller to provision out files */

@JsonIgnoreProperties(ignoreUnknown = true)
public final class OutputMetadata {
  private String outputDirectory;
  private String fileChecksum;
  private String fileChecksumType;

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setOutputDirectory(String outputDirectory) {
    this.outputDirectory = outputDirectory;
  }

  public String getFileChecksum() {
    return fileChecksum;
  }

  public void setFileChecksum(String fileChecksum) {
    this.fileChecksum = fileChecksum;
  }

  public String getFileChecksumType() {
    return fileChecksumType;
  }

  public void setFileChecksumType(String fileChecksumType) {
    this.fileChecksumType = fileChecksumType;
  }
}
