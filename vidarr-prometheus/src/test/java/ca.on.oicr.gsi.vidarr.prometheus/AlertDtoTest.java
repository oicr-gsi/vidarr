package ca.on.oicr.gsi.vidarr.prometheus;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

public class AlertDtoTest {
  private static final JsonMapper MAPPER =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .build();
  private final Set<String> configLabels = Stream.of("job", "scope").collect(Collectors.toSet());
  private final String autoInhibit = "AutoInhibit";

  @Test
  public void whenEnvAndJobMatch_matchesShouldMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = MAPPER.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "vidarr-clinical");
    labels.put("scope", "tele");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    List<String> matches =
        sut.matches(autoInhibit, "testing", configLabels, Stream.of("vidarr-clinical", "bamqc4"))
            .toList();
    assertEquals(1, matches.size());
  }

  @Test
  public void whenEnvAndWorkflowMatch_matchesShouldMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = MAPPER.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "bamqc4");
    labels.put("scope", "tele");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    List<String> matches =
        sut.matches(autoInhibit, "testing", configLabels, Stream.of("vidarr-clinical", "bamqc4"))
            .toList();
    assertEquals(1, matches.size());
  }

  @Test
  public void whenEnvAndScopeMatch_matchesShouldMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = MAPPER.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "bamqc-test-4");
    labels.put("scope", "bamqc4");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    List<String> matches =
        sut.matches(autoInhibit, "testing", configLabels, Stream.of("vidarr-clinical", "bamqc4"))
            .toList();
    assertEquals(1, matches.size());
  }

  @Test
  public void whenEnvDoesNotMatch_matchesShouldNotMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = MAPPER.createObjectNode();
    labels.put("environment", "outside");
    labels.put("job", "bamqc4");
    labels.put("scope", "tele");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    List<String> matches =
        sut.matches(autoInhibit, "testing", configLabels, Stream.of("vidarr-clinical", "bamqc4"))
            .toList();
    assertEquals(0, matches.size());
  }

  @Test
  public void whenAlertnameDoesNotMatch_matchesShouldNotMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = MAPPER.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "bamqc4");
    labels.put("scope", "tele");
    labels.put("alertname", "FullSpeedAhead");
    sut.setLabels(labels);

    List<String> matches =
        sut.matches(autoInhibit, "testing", configLabels, Stream.of("vidarr-clinical", "bamqc4"))
            .toList();
    assertEquals(0, matches.size());
  }

  @Test
  public void whenAlertScopeIsWorkflowRunId_matchesShouldMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = MAPPER.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "bamqc4");
    labels.put("scope", "615ed228fad3ae6193d5279dc689e83fa4225cd69c929e266dd84ef2ed96e719");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    List<String> matches =
        sut.matches(
                autoInhibit,
                "testing",
                configLabels,
                Stream.of(
                    "vidarr-clinical",
                    "615ed228fad3ae6193d5279dc689e83fa4225cd69c929e266dd84ef2ed96e719"))
            .toList();
    assertEquals(1, matches.size());
  }
}
