package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Set;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

/** Provision input using the stored path as the input path with no alteration */
public final class RawInputProvisioner extends BaseJsonInputProvisioner<String, String> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static InputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("raw", RawInputProvisioner.class));
  }

  private Set<InputProvisionFormat> formats;

  public RawInputProvisioner() {
    super(MAPPER, String.class, String.class);
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

  public Set<InputProvisionFormat> getFormats() {
    return formats;
  }

  @Override
  public void startup() {
    // Always ok.
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

  public void setFormats(Set<InputProvisionFormat> formats) {
    this.formats = formats;
  }
}
