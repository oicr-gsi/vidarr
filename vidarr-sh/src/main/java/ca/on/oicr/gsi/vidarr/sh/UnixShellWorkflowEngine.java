package ca.on.oicr.gsi.vidarr.sh;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import ca.on.oicr.gsi.vidarr.WorkMonitor.Status;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Run commands using UNIX shell locally */
public final class UnixShellWorkflowEngine
    extends BaseJsonWorkflowEngine<ShellState, String, String> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static WorkflowEngineProvider provider() {
    return new WorkflowEngineProvider() {
      @Override
      public WorkflowEngine readConfiguration(ObjectNode node) {
        return new UnixShellWorkflowEngine();
      }

      @Override
      public String type() {
        return "sh";
      }
    };
  }

  public UnixShellWorkflowEngine() {
    super(MAPPER, ShellState.class, String.class, String.class);
  }

  @Override
  public String cleanup(String cleanupState, WorkMonitor<Void, String> monitor) {
    recoverCleanup(cleanupState, monitor);
    return cleanupState;
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) {
    // Do nothing.
  }

  @Override
  public Optional<SimpleType> engineParameters() {
    return Optional.empty();
  }

  @Override
  public boolean supports(WorkflowLanguage language) {
    return language == WorkflowLanguage.UNIX_SHELL;
  }

  @Override
  public void recover(ShellState state, WorkMonitor<Result<String>, ShellState> monitor) {
    // When we recover, we have no way to recover a process's exit status, so we'll just assume it
    // exited successfully.
    ProcessHandle.of(state.getPid())
        .ifPresentOrElse(
            handle ->
                waitForCompletion(
                    monitor,
                    new File(state.getOutputPath()),
                    () -> handle.isAlive() ? OptionalInt.empty() : OptionalInt.of(0)),
            () -> monitor.permanentFailure("Cannot recover UNIX process between restarts."));
  }

  @Override
  protected void recoverCleanup(String path, WorkMonitor<Void, String> monitor) {
    monitor.scheduleTask(
        () -> {
          final var output = new File(path);
          if (output.exists()) {
            if (output.delete()) {
              System.err.printf("Failed to delete file: %s\n", output);
            }
          }
          monitor.complete(null);
        });
  }

  @Override
  public ShellState runWorkflow(
      WorkflowLanguage workflowLanguage,
      String workflow,
      String vidarrId,
      ObjectNode workflowParameters,
      JsonNode engineParameters,
      WorkMonitor<Result<String>, ShellState> monitor) {
    final var state = new ShellState();
    monitor.scheduleTask(
        () -> {
          try {
            final File outputFile = File.createTempFile("vidarr-sh", ".out");
            state.setOutputPath(outputFile.getAbsolutePath());
            monitor.storeRecoveryInformation(state);
            monitor.updateState(Status.WAITING);
            monitor.scheduleTask(
                () -> {
                  try {
                    final var process =
                        new ProcessBuilder()
                            .command("sh", "-c", workflow)
                            .redirectInput(Redirect.PIPE)
                            .redirectOutput(Redirect.to(outputFile))
                            .start();
                    try (final var stdin = process.getOutputStream()) {
                      MAPPER.writeValue(stdin, workflowParameters);
                    }
                    monitor.updateState(Status.RUNNING);
                    state.setPid(process.pid());
                    monitor.storeRecoveryInformation(state);
                    waitForCompletion(
                        monitor,
                        outputFile,
                        () ->
                            process.isAlive()
                                ? OptionalInt.empty()
                                : OptionalInt.of(process.exitValue()));
                  } catch (IOException e) {
                    monitor.permanentFailure(e.getMessage());
                  }
                });
          } catch (IOException e) {
            monitor.permanentFailure(e.getMessage());
          }
        });
    return state;
  }

  private void waitForCompletion(
      WorkMonitor<Result<String>, ShellState> monitor,
      File outputFile,
      Supplier<OptionalInt> checkExit) {
    monitor.scheduleTask(
        1,
        TimeUnit.MINUTES,
        new Runnable() {
          @Override
          public void run() {
            checkExit
                .get()
                .ifPresentOrElse(
                    exit -> {
                      if (exit == 0) {
                        try {
                          monitor.complete(
                              new Result<>(
                                  MAPPER.readTree(outputFile),
                                  outputFile.toURI().toASCIIString(),
                                  Optional.of(outputFile.getAbsolutePath())));
                        } catch (IOException e) {
                          monitor.permanentFailure(e.getMessage());
                        }

                      } else {
                        monitor.permanentFailure("Process exited with an error.");
                      }
                    },
                    () -> monitor.scheduleTask(1, TimeUnit.MINUTES, this));
          }
        });
  }
}
