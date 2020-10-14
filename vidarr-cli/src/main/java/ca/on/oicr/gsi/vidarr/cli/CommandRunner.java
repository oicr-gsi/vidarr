package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.core.ExternalId;
import ca.on.oicr.gsi.vidarr.core.OutputProvisioningHandler;
import ca.on.oicr.gsi.vidarr.core.WorkflowConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import picocli.CommandLine;

/** Subcommand to run a single workflow */
@CommandLine.Command(
    name = "run",
    description = "Run a workflow and dump the provisioning records to a file")
public class CommandRunner implements Callable<Integer> {
  private static ObjectNode read(String argument) throws IOException {
    if (argument.startsWith("@")) {
      return MAPPER.readValue(new File(argument.substring(1)), ObjectNode.class);
    } else {
      return MAPPER.readValue(argument, ObjectNode.class);
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @CommandLine.Option(
      names = {"-a", "--args", "--arguments"},
      required = true,
      description = "The arguments to feed the workflow (or @ to a file containing this)")
  private String arguments;

  @CommandLine.Option(
      names = {"-c", "--config", "--configuration"},
      required = true,
      description = "The target configuration to run against")
  private String configuration;

  @CommandLine.Option(
      names = {"-e", "--engine-arguments"},
      description = "The parameters to feed the workflow engine (or @ to a file containing this)")
  private String engineArguments = "{}";

  @CommandLine.Option(
      names = {"-m", "--meta", "--metadata"},
      required = true,
      description = "The output metadata to feed the workflow (or @ to a file containing this)")
  private String metadata;

  @CommandLine.Option(
      names = {"-o", "--output"},
      required = true,
      description = "File to write out provision out information")
  private String outputFile;

  @CommandLine.Option(
      names = {"-w", "--workflow"},
      required = true,
      description = "The workflow to run")
  private String workflowFile;

  @Override
  public Integer call() throws Exception {
    final var runner =
        new SingleShotProcessor(
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors()));
    final var target =
        MAPPER.readValue(new File(configuration), TargetConfiguration.class).toTarget();
    final var workflow =
        MAPPER.readValue(new File(workflowFile), WorkflowConfiguration.class).toDefinition();
    final var output = MAPPER.createArrayNode();
    switch (runner.run(
        "run",
        target,
        workflow,
        read(arguments),
        read(metadata),
        read(engineArguments),
        new OutputProvisioningHandler<>() {

          @Override
          public void provisionFile(
              Set<? extends ExternalId> ids,
              String storagePath,
              String md5,
              String metatype,
              Map<String, String> labels,
              SingleShotTransaction transaction) {
            System.err.println("Provisioning out file");
            final var node = output.objectNode();
            node.put("type", "file");
            node.put("storagePath", storagePath);
            node.put("md5", md5);
            node.put("metatype", metatype);
            final var outputIds = node.putArray("ids");
            for (final var id : ids) {
              final var outputId = outputIds.addObject();
              outputId.put("id", id.getId());
              outputId.put("provider", id.getProvider());
            }
            final var outputLabels = node.putObject("labels");
            for (final var label : labels.entrySet()) {
              outputLabels.put(label.getKey(), label.getValue());
            }
          }

          @Override
          public void provisionUrl(
              Set<? extends ExternalId> ids,
              String url,
              Map<String, String> labels,
              SingleShotTransaction transaction) {
            System.err.println("Provisioning out URL");
            final var node = output.objectNode();
            node.put("type", "url");
            node.put("url", url);
            final var outputIds = node.putArray("ids");
            for (final var id : ids) {
              final var outputId = outputIds.addObject();
              outputId.put("id", id.getId());
              outputId.put("provider", id.getProvider());
            }
            final var outputLabels = node.putObject("labels");
            for (final var label : labels.entrySet()) {
              outputLabels.put(label.getKey(), label.getValue());
            }
          }
        })) {
      case SUCCESS:
        MAPPER.writeValue(new File(outputFile), output);
        return 0;
      case FAILURE:
        System.err.println("Workflow failed.");
        return 1;
      case BAD_ARGUMENTS:
        System.err.println("Supplied arguments or metadata do not match workflow definition.");
        return 2;
      default:
        System.err.println("Internal error.");
        return 101;
    }
  }
}
