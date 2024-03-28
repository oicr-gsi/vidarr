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
public final class RawInputProvisioner implements InputProvisioner<RawInputState> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

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
  public RawInputState provision(WorkflowLanguage language, String id, String path) {
    return new RawInputState(JsonNodeFactory.instance.textNode(path));
  }

  @Override
  public RawInputState provisionExternal(WorkflowLanguage language, JsonNode metadata) {
    return new RawInputState(metadata);
  }

  @Override
  public OperationAction<?, RawInputState, JsonNode> run() {
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
