package ca.on.oicr.gsi.vidarr.prometheus;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import com.fasterxml.jackson.annotation.JsonIgnore;
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

  private final String alertName = "AutoInhibit";
  private String alertmanagerUrl;
  private String autoInhibitOnEnvironment;
  private Set<String> labelsOfInterest;
  private Set<String> valuesOfInterest;
  private Integer cacheTtl;
  private Integer cacheRequestTimeout;
  @JsonIgnore private AlertCache cache;

  public AlertmanagerAutoInhibitConsumableResource() {}

  public Integer getCacheTtl() {
    return cacheTtl;
  }

  public Integer getCacheRequestTimeout() {
    return cacheRequestTimeout;
  }

  public String getAlertmanagerUrl() {
    return alertmanagerUrl;
  }

  public String getAutoInhibitOnEnvironment() {
    return autoInhibitOnEnvironment;
  }

  public Set<String> getLabelsOfInterest() {
    return labelsOfInterest;
  }

  public Set<String> getValuesOfInterest() {
    return valuesOfInterest;
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
    if (cacheRequestTimeout == null) {
      throw new IllegalArgumentException(
          "The consumableResources 'alertmanager-auto-inhibit' config is missing "
              + "'cacheRequestTimeout': integer (minutes).");
    }
    if (cacheTtl == null) {
      throw new IllegalArgumentException(
          "The consumableResources 'alertmanager-auto-inhibit' config is missing "
              + "'cacheTtl': integer (minutes).");
    }
    if (labelsOfInterest.isEmpty()) {
      throw new IllegalArgumentException(
          "The consumableResources 'alertmanager-auto-inhibit' config is missing "
              + "'labelsOfInterest': [string].");
    }
    if (valuesOfInterest.isEmpty()) {
      throw new IllegalArgumentException(
          "The consumableResources 'alertmanager-auto-inhibit' config is missing "
              + "'valuesOfInterest': [string].");
    }
    cache = new AlertCache(name, alertmanagerUrl, cacheRequestTimeout, cacheTtl, MAPPER);
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
                        alertName,
                        autoInhibitOnEnvironment,
                        labelsOfInterest,
                        Stream.of(
                                valuesOfInterest.stream(),
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
              "Workflow %s has been throttled by %s alert(s) for: %s",
              alertName, workflowName, String.join(", ", isInhibited)));
    }
  }
}
