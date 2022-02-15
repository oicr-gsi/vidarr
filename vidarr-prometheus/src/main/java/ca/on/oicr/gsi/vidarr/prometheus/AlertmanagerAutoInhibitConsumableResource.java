package ca.on.oicr.gsi.vidarr.prometheus;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Allow throttling workflow runs if their parameters or the vidarr server's parameters match
 * existing AutoInhibit alerts in Prometheus Alertmanager. In order to throttle, an active
 * AutoInhibit alert must match 1) this resource's `environment` and 2) one or more of workflow
 * name, workflow name & version (as workflowName_version), or a fixed set of global inhibit values.
 */
public class AlertmanagerAutoInhibitConsumableResource implements ConsumableResource {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER = MAPPER.readerFor(new TypeReference<List<String>>() {});

  public static ConsumableResourceProvider provider() {
    return () ->
        Stream.of(
            new Pair<>(
                "alertmanager-auto-inhibit", AlertmanagerAutoInhibitConsumableResource.class));
  }

  private String alertmanagerUrl;
  private String autoInhibitOnEnvironment;
  private Set<String> labelsToCheck;
  private Set<String> inhibitOnValues;
  @JsonIgnore private AlertCache cache;

  public AlertmanagerAutoInhibitConsumableResource() {}

  public Set<String> getInhibitOnValues() {
    return inhibitOnValues;
  }

  public String getAlertmanagerUrl() {
    return alertmanagerUrl;
  }

  public String getAutoInhibitOnEnvironment() {
    return autoInhibitOnEnvironment;
  }

  public Set<String> getLabelsToCheck() {
    return labelsToCheck;
  }

  @Override
  public void startup(String name) {
    if (alertmanagerUrl == null || "".equals(alertmanagerUrl)) {
      throw new IllegalArgumentException(
          "The consumableResources 'alertmanager-auto-inhibit' config is missing "
              + "'alertmanagerUrl': string.");
    }
    if (autoInhibitOnEnvironment == null || "".equals(autoInhibitOnEnvironment)) {
      throw new IllegalArgumentException(
          "The consumableResources 'alertmanager-auto-inhibit' config is missing "
              + "'autoInhibitOnEnvironment': string.");
    }
    if (labelsToCheck.isEmpty()) {
      throw new IllegalArgumentException(
          "The consumableResources 'alertmanager-auto-inhibit' config is missing 'labelsToCheck': "
              + "[string].");
    }
    cache = new AlertCache(name, alertmanagerUrl, MAPPER);
  }

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return Optional.empty(); // This consumable resource operates only based on server config
  }

  @Override
  public void recover(String workflowName, String workflowVersion, String vidarrId) {
    // Do nothing, as there's no intermediate state that needs to be tracked
  }

  @Override
  public void release(String workflowName, String workflowVersion, String vidarrId) {
    // Do nothing, as this doesn't actually hold onto any resources
  }

  private Set<String> attemptDeserialization(JsonNode nodeInput) {
    try {
      return MAPPER.readValue(nodeInput.toString(), new TypeReference<>() {}); // Jackson dark magic
    } catch (JsonProcessingException e) {
      e.printStackTrace();

      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public ConsumableResourceResponse request(
      String workflowName, String workflowVersion, String vidarrId, Optional<JsonNode> input) {
    String workflowVersionWithUnderscores = workflowVersion.replaceAll("\\.", "_");
    final var isInhibited =
        cache
            .get()
            .flatMap(
                alert ->
                    alert.matches(
                        "AutoInhibit",
                        autoInhibitOnEnvironment,
                        labelsToCheck,
                        Stream.of(
                                inhibitOnValues.stream(),
                                Stream.of(workflowName),
                                Stream.of(
                                    String.format(
                                        "%s_%s", workflowName, workflowVersionWithUnderscores)))
                            .flatMap(Function.identity())))
            .collect(Collectors.toSet());
    if (isInhibited.isEmpty()) {
      return ConsumableResourceResponse.AVAILABLE;
    } else {
      return ConsumableResourceResponse.error(
          String.format(
              "Workflow %s has been throttled by AutoInhibit alert(s) for: %s",
              workflowName, String.join(", ", isInhibited)));
    }
  }
}
