package ca.on.oicr.gsi.vidarr.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine;

/** Main entry point from the command line */
@CommandLine.Command(
    name = "vidarr",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Vidarr workflow runner")
public class Main implements Callable<Integer> {
  public static void main(String[] args) {
    final var cmd =
        new CommandLine(new Main())
            .addSubcommand("run", new CommandRunner())
            .addSubcommand("test", new CommandTest());
    cmd.setExpandAtFiles(false);
    cmd.setExecutionStrategy(new CommandLine.RunLast());
    System.exit(cmd.execute(args));
  }

  @Override
  public Integer call() throws Exception {
    System.err.println("Please specify a command or --help to see what commands are available.");
    return 1;
  }
}
