package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.WorkMonitor;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.stream.Stream;
import javax.xml.stream.XMLStreamException;

/** An output provisioner that combines multiple other plugins using a tagged union */
public final class OneOfOutputProvisioner implements OutputProvisioner {

  public static OutputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("oneOf", OneOfOutputProvisioner.class));
  }

  private final Map<String, OutputProvisioner> provisioners;

  public OneOfOutputProvisioner(Map<String, OutputProvisioner> provisioners) {
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
  public JsonNode preflightCheck(JsonNode metadata, WorkMonitor<Boolean, JsonNode> monitor) {
    final var type = metadata.get("type").asText();
    return provisioners
        .get(type)
        .preflightCheck(metadata.get("contents"), new MonitorWithType<>(monitor, type));
  }

  @Override
  public void preflightRecover(JsonNode state, WorkMonitor<Boolean, JsonNode> monitor) {
    final var type = state.get(0).asText();
    provisioners.get(type).preflightRecover(state.get(1), new MonitorWithType<>(monitor, type));
  }

  @Override
  public JsonNode provision(
      String workflowRunId, String data, JsonNode metadata, WorkMonitor<Result, JsonNode> monitor) {
    final var type = metadata.get("type").asText();
    return provisioners
        .get(type)
        .provision(
            workflowRunId, data, metadata.get("contents"), new MonitorWithType<>(monitor, type));
  }

  @Override
  public void recover(JsonNode state, WorkMonitor<Result, JsonNode> monitor) {
    final var type = state.get(0).asText();
    provisioners.get(type).recover(state.get(1), new MonitorWithType<>(monitor, type));
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
