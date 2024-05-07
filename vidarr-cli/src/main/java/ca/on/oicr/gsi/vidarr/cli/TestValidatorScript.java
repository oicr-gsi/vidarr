package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public final class TestValidatorScript extends TestValidator {

  private String metricsCalculate;
  private String metricsCompare;
  private String outputMetrics;

  @Override
  public Validator createValidator(String outputDirectory, String id,
      String date, boolean verboseMode) {
    try {
      /*
      If output directory provided we will use that
      Note: within that directory we will have a subdirectory named after current epoch timestamp
      created inside Call() in commandTest.java
      else we create a new directory in the default temporary-file directory
      Note: That path is associate with the default FileSystem which is UNIX in our case
       */
      final var tempDir = Files.createTempDirectory("vidarr-test-script");
      final var finalDir = (outputDirectory != null) ? Path.of(outputDirectory)
          : tempDir;
      final var finalCalculateScript = finalDir.resolve(id + "_calculate_script_" + date);
      final var finalCalculateDir = finalDir.resolve(id + "_calculate_output_" + date);

      // If outputDirectory provided then output file name will be: "id.output"
      // Otherwise it will be: "calculate.output" if no output directory passed in
      final var finalOutputFile = (outputDirectory != null) ? id + ".output" : "calculate.output";

      // Directory creation
      Files.createDirectories(finalCalculateDir);
      Files.copy(Path.of(metricsCalculate), finalCalculateScript);
      Files.setPosixFilePermissions(
          finalCalculateScript,
          EnumSet.of(
              PosixFilePermission.OWNER_READ,
              PosixFilePermission.OWNER_WRITE,
              PosixFilePermission.OWNER_EXECUTE));
      return new Validator() {
        @Override
        public void provisionFile(
            Set<? extends ExternalId> ids,
            String storagePath,
            String md5,
            String metatype,
            long fileSize,
            Map<String, String> labels,
            Void transaction) {
          try {
            final var existing = Path.of(storagePath);
            Files.createSymbolicLink(finalCalculateDir.resolve(existing.getFileName()), existing);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }

        @Override
        public void provisionUrl(
            Set<? extends ExternalId> ids,
            String url,
            Map<String, String> labels,
            Void transaction) {
          // Not validated
        }

        @Override
        public boolean validate(String id) {
          try {
            // Additional information provided for user if verbose flag -v included
            if(verboseMode)
            {
              System.out.printf("Location of metrics calculate: %s \n", metricsCalculate);
              System.out.printf("Location of metrics compare: %s \n", metricsCompare);
              System.out.printf("Location of output metrics %s \n", outputMetrics);
            }

            // Directory where output file will be located
            final var output = Path.of(String.valueOf(finalDir), finalOutputFile).toAbsolutePath()
                .toFile();

            System.err.printf("%s: [%s] Calculating output to %s...%n", id, Instant.now(), output);
            final var calculateProcess =
                new ProcessBuilder()
                    .inheritIO()
                    .directory(finalCalculateDir.toFile())
                    .command(finalCalculateScript.toAbsolutePath().toString(),
                        finalCalculateDir.toString())
                    .redirectOutput(output)
                    .start();
            calculateProcess.waitFor();
            if (calculateProcess.exitValue() != 0) {
              System.err.printf(
                  "%s: [%s] Calculation script exited %d!%n",
                  id, Instant.now(), calculateProcess.exitValue());
              return false;
            }
            System.err.printf(
                "%s: [%s] Comparing output between %s and %s...%n",
                id, Instant.now(), outputMetrics, output);
            final var compareProcess =
                new ProcessBuilder()
                    .inheritIO()
                    .directory(finalDir.toFile())
                    .command(metricsCompare, outputMetrics, output.toString())
                    .start();
            compareProcess.waitFor();
            return compareProcess.exitValue() == 0;
          } catch (Exception e) {
            e.printStackTrace();
            return false;
          }
        }
      };
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @JsonProperty("metrics_calculate")
  public String getMetricsCalculate() {
    return metricsCalculate;
  }

  @JsonProperty("metrics_compare")
  public String getMetricsCompare() {
    return metricsCompare;
  }

  @JsonProperty("output_metrics")
  public String getOutputMetrics() {
    return outputMetrics;
  }

  public void setMetricsCalculate(String metricsCalculate) {
    this.metricsCalculate = metricsCalculate;
  }

  public void setMetricsCompare(String metricsCompare) {
    this.metricsCompare = metricsCompare;
  }

  public void setOutputMetrics(String outputMetrics) {
    this.outputMetrics = outputMetrics;
  }
}
