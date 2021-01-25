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
  public Validator createValidator() {
    try {
      final var tempDir = Files.createTempDirectory("vidarr-test-script");
      final var calculateScript = tempDir.resolve("calculate");
      final var calculateDir = tempDir.resolve("output");
      Files.createDirectories(calculateDir);
      Files.copy(Path.of(metricsCalculate), calculateScript);
      Files.setPosixFilePermissions(
          calculateScript,
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
            Files.createSymbolicLink(calculateDir.resolve(existing.getFileName()), existing);
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
            final var output = tempDir.resolve("calculate.output").toAbsolutePath().toFile();
            System.err.printf("%s: [%s] Calculating output to %s...%n", id, Instant.now(), output);
            final var calculateProcess =
                new ProcessBuilder()
                    .directory(calculateDir.toFile())
                    .command(calculateScript.toAbsolutePath().toString(), calculateDir.toString())
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
                    .directory(tempDir.toFile())
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
