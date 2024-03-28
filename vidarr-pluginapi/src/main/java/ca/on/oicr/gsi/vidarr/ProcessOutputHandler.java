package ca.on.oicr.gsi.vidarr;

import static ca.on.oicr.gsi.vidarr.OperationAction.MAPPER;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Determines how the standard output of a process is handled
 *
 * @param <Output> the type of data that will be returned to the operation
 */
public abstract sealed class ProcessOutputHandler<Output>
    permits ProcessOutputToStandardOutput, ProcessOutputToFile, ProcessOutputAsJson {
  public interface OutputGenerator<Output> {
    Output get(boolean success) throws Exception;
  }

  /**
   * Parse standard output as a JSON object
   *
   * @param clazz a Java class to indicate the kind of JSON data
   * @param successOnly if true and the process exists with non-zero exit status, the output will
   *     not attempt to be parsed and a null value will be returned instead.
   * @return a handler that will parse the JSON data
   * @param <Body> the type of JSON data being parsed
   */
  public static <Body> ProcessOutputHandler<Body> readOutput(
      Class<Body> clazz, boolean successOnly) {
    return new ProcessOutputAsJson<>(MAPPER.getTypeFactory().constructType(clazz), successOnly);
  }

  /**
   * Parse standard output as a JSON object
   *
   * @param typeReference a type reference to indicate the kind of JSON data
   * @param successOnly if true and the process exists with non-zero exit status, the output will
   *     not attempt to be parsed and a null value will be returned instead.
   * @return a handler that will parse the JSON data
   * @param <Body> the type of JSON data being parsed
   */
  public static <Body> ProcessOutputHandler<Body> readOutput(
      TypeReference<Body> typeReference, boolean successOnly) {
    return new ProcessOutputAsJson<>(
        MAPPER.getTypeFactory().constructType(typeReference), successOnly);
  }

  /**
   * Write the data to a temporary file and provide the path to that file
   *
   * @return handler that will cause the child process to write to a file; if the file is not
   *     deleted, it will be deleted on JVM exit
   */
  public static ProcessOutputHandler<Path> toFile() {
    return new ProcessOutputToFile();
  }

  /**
   * Write the data to Vidarr's standard output and provide nothing
   *
   * @return handler that will cause the child process to inherit the server's standard output
   */
  public static ProcessOutputHandler<Void> toStandardOutput() {
    return new ProcessOutputToStandardOutput();
  }

  abstract OutputGenerator<Output> prepare(ProcessBuilder build) throws IOException;
}
