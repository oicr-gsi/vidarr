package ca.on.oicr.gsi.vidarr;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

final class ProcessOutputToFile extends ProcessOutputHandler<Path> {

  @Override
  public OutputGenerator<Path> prepare(ProcessBuilder build) throws IOException {
    final var output = File.createTempFile("vidarr", ".out");
    output.deleteOnExit();
    return (success) -> output.toPath();
  }
}
