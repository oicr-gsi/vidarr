package ca.on.oicr.gsi.vidarr.prometheus;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class AlertDtoTest {
  private final ObjectMapper mapper = new ObjectMapper();

  private final Set<String> configLabels = Stream.of("job", "scope").collect(Collectors.toSet());

  @Test
  public void whenEnvAndJobMatch_matchesShouldMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = mapper.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "vidarr-clinical");
    labels.put("scope", "tele");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    var matches =
        sut.matches("testing", Stream.of("vidarr-clinical", "bamqc4"), configLabels)
            .collect(Collectors.toList());
    assertEquals(1, matches.size());
  }

  @Test
  public void whenEnvAndWorkflowMatch_matchesShouldMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = mapper.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "bamqc4");
    labels.put("scope", "tele");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    var matches =
        sut.matches("testing", Stream.of("vidarr-clinical", "bamqc4"), configLabels)
            .collect(Collectors.toList());
    assertEquals(1, matches.size());
  }

  @Test
  public void whenEnvAndScopeMatch_matchesShouldMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = mapper.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "bamqc-test-4");
    labels.put("scope", "bamqc4");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    var matches =
        sut.matches("testing", Stream.of("vidarr-clinical", "bamqc4"), configLabels)
            .collect(Collectors.toList());
    assertEquals(1, matches.size());
  }

  @Test
  public void whenEnvDoesNotMatch_matchesShouldNotMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = mapper.createObjectNode();
    labels.put("environment", "outside");
    labels.put("job", "bamqc4");
    labels.put("scope", "tele");
    labels.put("alertname", "AutoInhibit");
    sut.setLabels(labels);

    var matches =
        sut.matches("testing", Stream.of("vidarr-clinical", "bamqc4"), configLabels)
            .collect(Collectors.toList());
    assertEquals(0, matches.size());
  }

  @Test
  public void whenAlertnameDoesNotMatch_matchesShouldNotMatch() {
    AlertDto sut = new AlertDto();

    ObjectNode labels = mapper.createObjectNode();
    labels.put("environment", "testing");
    labels.put("job", "bamqc4");
    labels.put("scope", "tele");
    labels.put("alertname", "FullSpeedAhead");
    sut.setLabels(labels);

    var matches =
        sut.matches("testing", Stream.of("vidarr-clinical", "bamqc4"), configLabels)
            .collect(Collectors.toList());
    assertEquals(0, matches.size());
  }
}
