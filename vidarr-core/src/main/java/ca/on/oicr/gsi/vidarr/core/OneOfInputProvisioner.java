package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.InputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import ca.on.oicr.gsi.vidarr.WorkflowLanguage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Map;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

/** An input provisioner that selects between multiple input provisioners using a tagged union */
public final class OneOfInputProvisioner implements InputProvisioner {
  public static InputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("oneOf", OneOfInputProvisioner.class));
  }

  private String internal;
  private Map<String, InputProvisioner> provisioners;

  public OneOfInputProvisioner(Map<String, InputProvisioner> provisioners, String internal) {
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

  public Map<String, InputProvisioner> getProvisioners() {
    return provisioners;
  }

  @Override
  public JsonNode provision(
      WorkflowLanguage language, String id, String path, WorkMonitor<JsonNode, JsonNode> monitor) {
    final var output = JsonNodeFactory.instance.arrayNode();
    output.add(internal);
    output.add(
        provisioners
            .get(internal)
            .provision(language, id, path, new MonitorWithType<>(monitor, internal)));
    return output;
  }

  @Override
  public JsonNode provisionExternal(
      WorkflowLanguage language, JsonNode metadata, WorkMonitor<JsonNode, JsonNode> monitor) {
    final var type = metadata.get("type").asText();
    final var output = JsonNodeFactory.instance.arrayNode();
    output.add(type);
    output.add(
        provisioners
            .get(type)
            .provisionExternal(
                language, metadata.get("contents"), new MonitorWithType<>(monitor, type)));
    return output;
  }

  @Override
  public void recover(JsonNode state, WorkMonitor<JsonNode, JsonNode> monitor) {
    final var type = state.get(0).asText();
    provisioners.get(type).recover(state.get(1), new MonitorWithType<>(monitor, type));
  }

  public void setInternal(String internal) {
    this.internal = internal;
  }

  public void setProvisioners(Map<String, InputProvisioner> provisioners) {
    this.provisioners = provisioners;
  }

  @Override
  public void startup() {
    if (!provisioners.containsKey(internal)) {
      throw new RuntimeException("The internal provider name is not known.");
    }
  }
}
