package ca.on.oicr.gsi.vidarr.sh;

/** The shell workflow engine's state to be stored in the database */
public final class ShellState {
  private String outputPath;
  private long pid;

  public String getOutputPath() {
    return outputPath;
  }

  public long getPid() {
    return pid;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }

  public void setPid(long pid) {
    this.pid = pid;
  }
}
