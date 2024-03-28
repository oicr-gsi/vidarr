package ca.on.oicr.gsi.vidarr;

import java.lang.ProcessBuilder.Redirect;

final class ProcessOutputToStandardOutput extends ProcessOutputHandler<Void> {

  @Override
  public OutputGenerator<Void> prepare(ProcessBuilder build) {
    build.redirectOutput(Redirect.INHERIT);
    return (success) -> null;
  }
}
