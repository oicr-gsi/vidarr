package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OperationAction.BranchState;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

/** An output provisioner that combines multiple other plugins using a tagged union */
public final class OneOfOutputProvisioner implements OutputProvisioner<BranchState, BranchState> {

  public static OutputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("oneOf", OneOfOutputProvisioner.class));
  }

  private final Map<String, OutputProvisioner<?, ?>> provisioners;

  public OneOfOutputProvisioner(Map<String, OutputProvisioner<?, ?>> provisioners) {
    this.provisioners = provisioners;
  }

  @Override
  public boolean canProvision(OutputProvisionFormat format) {
    return provisioners.values().stream().allMatch(p -> p.canProvision(format));
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    for (final var provider : provisioners.entrySet()) {
      sectionRenderer.line("Type", provider.getKey());
      provider.getValue().configuration(sectionRenderer);
    }
  }

  @Override
  public BranchState preflightCheck(JsonNode metadata) {
    final var type = metadata.get("type").asText();
    return preflightCheck(type, provisioners.get(type), metadata.get("contents"));
  }

  private <PreflightState extends Record> BranchState preflightCheck(
      String type, OutputProvisioner<PreflightState, ?> provisioner, JsonNode metadata) {
    return provisioner.runPreflight().intoBranch(type, provisioner.preflightCheck(metadata));
  }

  @Override
  public BranchState provision(String workflowRunId, String data, JsonNode metadata) {
    final var type = metadata.get("type").asText();
    return provision(type, provisioners.get(type), workflowRunId, data, metadata.get("contents"));
  }

  private <State extends Record> BranchState provision(
      String type,
      OutputProvisioner<?, State> provisioner,
      String workflowRunId,
      String data,
      JsonNode metadata) {
    return provisioner.run().intoBranch(type, provisioner.provision(workflowRunId, data, metadata));
  }

  @Override
  public OperationAction<?, BranchState, Result> run() {
    return OperationAction.branch(
        provisioners.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().run())));
  }

  @Override
  public OperationAction<?, BranchState, Boolean> runPreflight() {
    return OperationAction.branch(
        provisioners.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().runPreflight())));
  }

  @Override
  public void startup() {
    for (final var provisioner : provisioners.values()) {
      provisioner.startup();
    }
  }

  @Override
  public String type() {
    return "oneOf";
  }

  @Override
  public BasicType typeFor(OutputProvisionFormat format) {
    return BasicType.taggedUnion(
        provisioners.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().typeFor(format))));
  }
}
