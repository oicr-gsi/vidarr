package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.core.BaseProcessor;
import ca.on.oicr.gsi.vidarr.core.WorkflowConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import picocli.CommandLine;

/**
 * Subcommand to running unit tests
 */
@CommandLine.Command(name = "test", description = "Run a test suite for a workflow")
public class CommandTest implements Callable<Integer> {

  private static ObjectNode read(String argument) throws IOException {
    if (argument.startsWith("@")) {
      return MAPPER.readValue(new File(argument.substring(1)), ObjectNode.class);
    } else {
      return MAPPER.readValue(argument, ObjectNode.class);
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @CommandLine.Option(
      names = {"-c", "--config", "--configuration"},
      required = true,
      description = "The target configuration to run against")
  private String configuration;

  @CommandLine.Option(
      names = {"-i", "--include"},
      description = "Limit to test to only the listed tests")
  private List<String> includes = List.of();

  @CommandLine.Option(
      names = {"-t", "--test"},
      required = true,
      description = "File containing the test case")
  private String testCases;

  @CommandLine.Option(
      names = {"-w", "--workflow"},
      required = true,
      description = "The workflow to run")
  private String workflowFile;

  @CommandLine.Option(
      names = {"-o", "--output"},
      description = "Location of directory to write test output")
  private String outputDirectory;

  @CommandLine.Option(
      names = {"-v", "--verbose"},
      description = "Verbose mode. Helpful for troubleshooting")
  private boolean verboseMode;

  @Override
  public Integer call() throws Exception {
    // Get current epoch timestamp and format it to date
    final long epoch = System.currentTimeMillis();
    final String date = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss").format(new Date(epoch));

    final var suffix = Instant.now().getEpochSecond();
    final var target =
        MAPPER.readValue(new File(configuration), TargetConfiguration.class).toTarget();
    final var workflow =
        MAPPER.readValue(new File(workflowFile), WorkflowConfiguration.class).toDefinition();
    var cases = List.of(MAPPER.readValue(new File(testCases), TestCase[].class));
    if (!includes.isEmpty()) {
      cases = cases.stream().filter(c -> includes.contains(c.getId())).toList();
    }
    if (cases.isEmpty()) {
      System.err.println("No test cases found.");
      return 1;
    }
    final var nameCounts =
        cases.stream()
            .map(TestCase::getId)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    if (nameCounts.values().stream().anyMatch(c -> c > 1)) {
      System.err.println(
          nameCounts.entrySet().stream()
              .filter(e -> e.getValue() > 1)
              .map(e -> String.format("%s (%d)", e.getKey(), e.getValue()))
              .collect(Collectors.joining(", ", "Test cases do not have unique ids: ", "")));
      return 1;
    }

    final var executor =
        Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    final var runner = new SingleShotProcessor(executor);
    final List<String> errors =
        cases.stream()
            .flatMap(
                c ->
                    BaseProcessor.validateInput(
                            MAPPER,
                            target,
                            workflow,
                            c.getArguments(),
                            c.getMetadata(),
                            c.getEngineArguments())
                        .map((c.getId() + ": ")::concat))
            .toList();
    if (!errors.isEmpty()) {
      errors.forEach(System.err::println);
      return 2;
    }

    var result = true;
    for (final var future :
        cases.stream()
            .map(
                c -> {
                  /* Will use output directory if provided, otherwise "null" is passed into
                     createValidator Timestamp date passed in to use as subdirectory to output
                     directory.One is created for each vidarr-cli test run */
                  final var validator =
                      Validator.all(c.getValidators().stream().map(
                          TestValidator -> TestValidator.createValidator(outputDirectory,
                              c.getId(), date, verboseMode)));

                  final var run =
                      runner.startAsync(
                          String.format("%s-%d", c.getId(), suffix),
                          target,
                          workflow,
                          c.getArguments(),
                          c.getMetadata(),
                          c.getEngineArguments(),
                          validator);
                  if (run == null) {
                    System.err.printf("%s: [%s] Test failed to launch%n", c.getId(), Instant.now());
                    return CompletableFuture.completedFuture(false);
                  }
                  return run.future()
                      .thenApply(
                          v -> {
                            if (v) {
                              System.err.printf(
                                  "%s: [%s] Workflow completed. Starting validation...%n",
                                  c.getId(), Instant.now());
                              if (validator.validate(c.getId())) {
                                System.err.printf(
                                    "%s: [%s] Test completed and validated%n",
                                    c.getId(), Instant.now());
                                return true;
                              } else {
                                System.err.printf(
                                    "%s: [%s] Test completed and failed validation%n",
                                    c.getId(), Instant.now());
                                return false;
                              }
                            } else {
                              System.err.printf(
                                  "%s: [%s] Workflow failed.%n", c.getId(), Instant.now());
                              return false;
                            }
                          });
                })
            .toList()) {
      result &= future.join();
    }
    return result ? 0 : 1;
  }
}
