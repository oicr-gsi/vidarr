package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.EnumSet;
import java.util.Set;
import javax.xml.stream.XMLStreamException;

/** Provision input using the stored path as the input path with no alteration */
public final class RawInputProvisioner extends BaseJsonInputProvisioner<String, String> {
  public static InputProvisionerProvider provider() {
    return new InputProvisionerProvider() {
      @Override
      public InputProvisioner readConfiguration(ObjectNode node) {
        final var formats = EnumSet.noneOf(InputProvisionFormat.class);
        for (final var element : node.get("formats")) {
          formats.add(InputProvisionFormat.valueOf(element.asText()));
        }
        return new RawInputProvisioner(formats);
      }

      @Override
      public String type() {
        return "raw";
      }
    };
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final Set<InputProvisionFormat> formats;

  protected RawInputProvisioner(Set<InputProvisionFormat> formats) {
    super(MAPPER, String.class, String.class);
    this.formats = formats;
  }

  @Override
  public boolean canProvision(InputProvisionFormat format) {
    return formats.contains(format);
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    // Do nothing.
  }

  @Override
  public BasicType externalTypeFor(InputProvisionFormat format) {
    if (formats.contains(format)) {
      return BasicType.STRING;
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  protected String provisionExternal(
      WorkflowLanguage language, String metadata, WorkMonitor<JsonNode, String> monitor) {
    monitor.scheduleTask(() -> monitor.complete(JsonNodeFactory.instance.textNode(metadata)));
    return metadata;
  }

  @Override
  public String provisionRegistered(
      WorkflowLanguage language, String id, String path, WorkMonitor<JsonNode, String> monitor) {
    monitor.scheduleTask(() -> monitor.complete(JsonNodeFactory.instance.textNode(path)));
    return path;
  }

  @Override
  protected void recover(String state, WorkMonitor<JsonNode, String> monitor) {
    monitor.scheduleTask(() -> monitor.complete(JsonNodeFactory.instance.textNode(state)));
  }
}
