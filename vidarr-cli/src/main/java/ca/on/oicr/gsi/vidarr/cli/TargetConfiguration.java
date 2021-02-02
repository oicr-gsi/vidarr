package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.*;
import ca.on.oicr.gsi.vidarr.core.Target;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Configuration file format for workflow testing */
public final class TargetConfiguration {
  private static final Map<String, WorkflowEngineProvider> ENGINES =
      ServiceLoader.load(WorkflowEngineProvider.class).stream()
          .map(ServiceLoader.Provider::get)
          .collect(Collectors.toMap(WorkflowEngineProvider::type, Function.identity()));
  private static final Map<String, InputProvisionerProvider> INPUT_PROVISIONERS =
      ServiceLoader.load(InputProvisionerProvider.class).stream()
          .map(ServiceLoader.Provider::get)
          .collect(Collectors.toMap(InputProvisionerProvider::type, Function.identity()));
  private static final Map<String, OutputProvisionerProvider> OUTPUT_PROVISIONERS =
      ServiceLoader.load(OutputProvisionerProvider.class).stream()
          .map(ServiceLoader.Provider::get)
          .collect(Collectors.toMap(OutputProvisionerProvider::type, Function.identity()));
  private static final Map<String, RuntimeProvisionerProvider> RUNTIME_PROVISIONERS =
      ServiceLoader.load(RuntimeProvisionerProvider.class).stream()
          .map(ServiceLoader.Provider::get)
          .collect(Collectors.toMap(RuntimeProvisionerProvider::name, Function.identity()));
  private ObjectNode engine;
  private List<ObjectNode> inputs;
  private List<ObjectNode> outputs;
  private List<ObjectNode> runtimes = Collections.emptyList();

  public ObjectNode getEngine() {
    return engine;
  }

  public List<ObjectNode> getInputs() {
    return inputs;
  }

  public List<ObjectNode> getOutputs() {
    return outputs;
  }

  public List<ObjectNode> getRuntimes() {
    return runtimes;
  }

  public void setEngine(ObjectNode engine) {
    this.engine = engine;
  }

  public void setInputs(List<ObjectNode> inputs) {
    this.inputs = inputs;
  }

  public void setOutputs(List<ObjectNode> outputs) {
    this.outputs = outputs;
  }

  public void setRuntimes(List<ObjectNode> runtimes) {
    this.runtimes = runtimes;
  }

  public Target toTarget() {
    return new Target() {
      final WorkflowEngine engine =
          ENGINES
              .get(TargetConfiguration.this.engine.get("type").asText())
              .readConfiguration(TargetConfiguration.this.engine);
      final Map<InputProvisionFormat, InputProvisioner> inputs =
          TargetConfiguration.this.inputs.stream()
              .map(
                  i -> {
                    final var name = i.get("type").asText();
                    final var provider = INPUT_PROVISIONERS.get(name);
                    if (provider == null) {
                      throw new IllegalArgumentException("Unknown input provider: " + name);
                    }
                    return provider.readConfiguration(i);
                  })
              .flatMap(
                  p ->
                      Stream.of(InputProvisionFormat.values())
                          .filter(p::canProvision)
                          .map(f -> new Pair<>(f, p)))
              .collect(Collectors.toMap(Pair::first, Pair::second));

      final Map<OutputProvisionFormat, OutputProvisioner> outputs =
          TargetConfiguration.this.outputs.stream()
              .map(
                  o -> {
                    final var name = o.get("type").asText();
                    final var provider = OUTPUT_PROVISIONERS.get(name);
                    if (provider == null) {
                      throw new IllegalArgumentException("Unknown output provider: " + name);
                    }
                    return provider.readConfiguration(o);
                  })
              .flatMap(
                  p ->
                      Stream.of(OutputProvisionFormat.values())
                          .filter(p::canProvision)
                          .map(f -> new Pair<>(f, p)))
              .collect(Collectors.toMap(Pair::first, Pair::second));

      final List<RuntimeProvisioner> runtimes =
          TargetConfiguration.this.runtimes.stream()
              .map(
                  c -> {
                    final var name = c.get("type").asText();
                    final var provider = RUNTIME_PROVISIONERS.get(name);
                    if (provider == null) {
                      throw new IllegalArgumentException("Unknown runtime provider: " + name);
                    }
                    return provider.readConfiguration(c);
                  })
              .collect(Collectors.toList());

      @Override
      public Stream<ConsumableResource> consumableResources() {
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
