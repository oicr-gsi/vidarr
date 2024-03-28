package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.InputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OperationAction.BranchState;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

/** An input provisioner that selects between multiple input provisioners using a tagged union */
public final class OneOfInputProvisioner implements InputProvisioner<BranchState> {
  public static InputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("oneOf", OneOfInputProvisioner.class));
  }

  private String internal;
  private Map<String, InputProvisioner<? extends Record>> provisioners;

  public OneOfInputProvisioner(
      Map<String, InputProvisioner<? extends Record>> provisioners, String internal) {
    this.provisioners = provisioners;
    this.internal = internal;
  }

  @Override
  public boolean canProvision(InputProvisionFormat format) {
    return provisioners.values().stream().allMatch(i -> i.canProvision(format));
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    for (final var provider : provisioners.entrySet()) {
      sectionRenderer.line("Type", provider.getKey());
      provider.getValue().configuration(sectionRenderer);
    }
  }

  @Override
  public BasicType externalTypeFor(InputProvisionFormat format) {
    return BasicType.taggedUnion(
        provisioners.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().externalTypeFor(format))));
  }

  public String getInternal() {
    return internal;
  }

  public Map<String, InputProvisioner<? extends Record>> getProvisioners() {
    return provisioners;
  }

  @Override
  public BranchState provision(WorkflowLanguage language, String id, String path) {
    return provision(provisioners.get(internal), language, id, path);
  }

  private <OriginalState extends Record> BranchState provision(
      InputProvisioner<OriginalState> provisioner,
      WorkflowLanguage language,
      String id,
      String path) {
    return provisioner.run().intoBranch(internal, provisioner.provision(language, id, path));
  }

  @Override
  public BranchState provisionExternal(WorkflowLanguage language, JsonNode metadata) {
    final var type = metadata.get("type").asText();
    return provisionExternal(type, provisioners.get(type), language, metadata.get("contents"));
  }

  private <OriginalState extends Record> BranchState provisionExternal(
      String name,
      InputProvisioner<OriginalState> provisioner,
      WorkflowLanguage language,
      JsonNode metadata) {
    return provisioner.run().intoBranch(name, provisioner.provisionExternal(language, metadata));
  }

  @Override
  public OperationAction<?, BranchState, JsonNode> run() {
    return OperationAction.branch(
        provisioners.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().run())));
  }

  public void setInternal(String internal) {
    this.internal = internal;
  }

  public void setProvisioners(Map<String, InputProvisioner<? extends Record>> provisioners) {
    this.provisioners = provisioners;
  }

  @Override
  public void startup() {
    if (!provisioners.containsKey(internal)) {
      throw new RuntimeException("The internal provider name is not known.");
    }
  }
}
