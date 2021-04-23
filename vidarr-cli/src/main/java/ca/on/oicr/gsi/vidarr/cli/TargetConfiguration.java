package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.*;
import ca.on.oicr.gsi.vidarr.core.Target;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Configuration file format for workflow testing */
public final class TargetConfiguration {
  private WorkflowEngine engine;
  private List<InputProvisioner> inputs;
  private List<OutputProvisioner> outputs;
  private List<RuntimeProvisioner> runtimes = Collections.emptyList();

  public WorkflowEngine getEngine() {
    return engine;
  }

  public List<InputProvisioner> getInputs() {
    return inputs;
  }

  public List<OutputProvisioner> getOutputs() {
    return outputs;
  }

  public List<RuntimeProvisioner> getRuntimes() {
    return runtimes;
  }

  public void setEngine(WorkflowEngine engine) {
    this.engine = engine;
  }

  public void setInputs(List<InputProvisioner> inputs) {
    this.inputs = inputs;
  }

  public void setOutputs(List<OutputProvisioner> outputs) {
    this.outputs = outputs;
  }

  public void setRuntimes(List<RuntimeProvisioner> runtimes) {
    this.runtimes = runtimes;
  }

  public Target toTarget() {
    for (final var input : inputs) {
      input.startup();
    }
    for (final var output : outputs) {
      output.startup();
    }
    for (final var runtime : runtimes) {
      runtime.startup();
    }
    engine.startup();
    return new Target() {
      final Map<InputProvisionFormat, InputProvisioner> inputs =
          TargetConfiguration.this.inputs.stream()
              .flatMap(
                  p ->
                      Stream.of(InputProvisionFormat.values())
                          .filter(p::canProvision)
                          .map(f -> new Pair<>(f, p)))
              .collect(Collectors.toMap(Pair::first, Pair::second));

      final Map<OutputProvisionFormat, OutputProvisioner> outputs =
          TargetConfiguration.this.outputs.stream()
              .flatMap(
                  p ->
                      Stream.of(OutputProvisionFormat.values())
                          .filter(p::canProvision)
                          .map(f -> new Pair<>(f, p)))
              .collect(Collectors.toMap(Pair::first, Pair::second));

      @Override
      public Stream<Pair<String, ConsumableResource>> consumableResources() {
        return Stream.empty();
      }

      @Override
      public WorkflowEngine engine() {
        return engine;
      }

      @Override
      public InputProvisioner provisionerFor(InputProvisionFormat type) {
        return inputs.get(type);
      }

      @Override
      public OutputProvisioner provisionerFor(OutputProvisionFormat type) {
        return outputs.get(type);
      }

      @Override
      public Stream<RuntimeProvisioner> runtimeProvisioners() {
        return runtimes.stream();
      }
    };
  }
}
