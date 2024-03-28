package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationAction.MAPPER;

import com.fasterxml.jackson.databind.JavaType;
import java.io.File;
import java.io.IOException;

final class ProcessOutputAsJson<Body> extends ProcessOutputHandler<Body> {

  private final boolean successOnly;
  private final JavaType type;

  public ProcessOutputAsJson(JavaType type, boolean successOnly) {
    this.type = type;
    this.successOnly = successOnly;
  }

  @Override
  public OutputGenerator<Body> prepare(ProcessBuilder build) throws IOException {
    final var output = File.createTempFile("vidarr", ".out");
    output.deleteOnExit();
    return (boolean success) -> {
      if (!success && successOnly) {
        return null;
      }
      try {
        return MAPPER.readValue(output, type);
      } finally {
        output.delete();
      }
    };
  }
}
