package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import javax.xml.stream.XMLStreamException;

/** An input provisioner that selects between multiple input provisioners using a tagged union */
public final class OneOfInputProvisioner implements InputProvisioner {
  public static InputProvisionerProvider provider() {
    return new InputProvisionerProvider() {
      @Override
      public InputProvisioner readConfiguration(ObjectNode node) {
        final var internal = node.get("internal").asText().toUpperCase();
        final var iterator = node.get("provisioners").fields();
        final var provisioners = new TreeMap<String, InputProvisioner>();
        while (iterator.hasNext()) {
          final var item = iterator.next();
          final var type = item.getValue().get("type").asText();
          provisioners.put(
              item.getKey().toUpperCase(),
              PROVIDERS.stream()
                  .map(ServiceLoader.Provider::get)
                  .filter(p -> p.type().equals(type))
                  .findAny()
                  .orElseThrow(
                      () ->
                          new IllegalArgumentException(
                              String.format("Unknown input provider: %s", type)))
                  .readConfiguration((ObjectNode) item.getValue()));
        }
        if (!provisioners.containsKey(internal)) {
          throw new IllegalArgumentException(
              String.format(
                  "Internal provision format %s is not configured: %s",
                  internal, String.join(", ", provisioners.keySet())));
        }
        return new OneOfInputProvisioner(provisioners, internal);
      }

      @Override
      public String type() {
        return "oneOf";
      }
    };
  }

  private static final ServiceLoader<InputProvisionerProvider> PROVIDERS =
      ServiceLoader.load(InputProvisionerProvider.class);
  private final String internal;
  private final Map<String, InputProvisioner> provisioners;

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
}
