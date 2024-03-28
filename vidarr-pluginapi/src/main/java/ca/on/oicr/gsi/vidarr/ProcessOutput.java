package ca.on.oicr.gsi.vidarr;

/**
 * The output status information from a locally-run process/program
 *
 * @param exitCode the process's final exit code
 * @param standardOutput the data gathered from standard output
 * @param <Body> the type of the standard output data
 */
public record ProcessOutput<Body>(int exitCode, Body standardOutput) {

  /**
   * Checks if the process exited successfully
   *
   * @return true if the exit code is zero
   */
  public boolean success() {
    return exitCode == 0;
  }
}
