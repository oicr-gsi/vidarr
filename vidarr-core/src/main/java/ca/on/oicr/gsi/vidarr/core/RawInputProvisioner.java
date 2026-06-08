package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.*;
import java.util.Set;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.StringNode;

/** Provision input using the stored path as the input path with no alteration */
public final class RawInputProvisioner implements InputProvisioner<RawInputState> {
  private static final JsonMapper MAPPER =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .build();

  public static InputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("raw", RawInputProvisioner.class));
  }

  private Set<InputProvisionFormat> formats;

  public RawInputProvisioner() {}

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
  public RawInputState prepareInternalProvisionInput(
      WorkflowLanguage language, String id, String path) {
    return new RawInputState(StringNode.valueOf(path));
  }

  @Override
  public RawInputState prepareExternalProvisionInput(WorkflowLanguage language, JsonNode metadata) {
    return new RawInputState(metadata);
  }

  @Override
  public OperationAction<?, RawInputState, JsonNode> build() {
    return OperationAction.load(RawInputState.class, RawInputState::path)
        .then(OperationStep.require(JsonNode::isTextual, "Input is not text"));
  }

  public Set<InputProvisionFormat> getFormats() {
    return formats;
  }

  @Override
  public void startup() {
    // Always ok.
  }

  public void setFormats(Set<InputProvisionFormat> formats) {
    this.formats = formats;
  }
}
