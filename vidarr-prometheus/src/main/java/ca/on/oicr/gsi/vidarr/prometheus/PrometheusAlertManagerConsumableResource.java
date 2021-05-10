package ca.on.oicr.gsi.vidarr.prometheus;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.cache.ReplacingRecord;
import ca.on.oicr.gsi.cache.ValueCache;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.ConsumableResourceProvider;
import ca.on.oicr.gsi.vidarr.ConsumableResourceResponse;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PrometheusAlertManagerConsumableResource implements ConsumableResource {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectReader READER = MAPPER.readerFor(new TypeReference<List<String>>() {});
  private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

  public static ConsumableResourceProvider provider() {
    return () ->
        Stream.of(
            new Pair<>("prometheus-alert-manager", PrometheusAlertManagerConsumableResource.class));
  }

  private Set<String> fixedVidarrNames;
  private String alertManagerUrl;
  private String environment;
  private Set<String> labels;
  @JsonIgnore private AlertCache cache;

  public PrometheusAlertManagerConsumableResource() {}

  public Set<String> getFixedVidarrNames() {
    return fixedVidarrNames;
  }

  public String getAlertManagerUrl() {
    return alertManagerUrl;
  }

  public String getEnvironment() {
    return environment;
  }

  public Set<String> getLabels() {
    return labels;
  }

  @Override
  public void startup(String name) {
    if (fixedVidarrNames.isEmpty()) {
      throw new IllegalArgumentException(
          "Fixed Vidarr names is empty in Prometheus Alertmanager resource config.");
    }
    if (alertManagerUrl == null || "".equals(alertManagerUrl)) {
      throw new IllegalArgumentException(
          "Alertmanager URL is missing in Prometheus Alertmanager resource config.");
    }
    if (environment == null || "".equals(environment)) {
      throw new IllegalArgumentException(
          "Environment is missing in Prometheus Alertmanager resource config.");
    }
    if (labels.isEmpty()) {
      throw new IllegalArgumentException(
          "Labels are missing in Prometheus Alertmanager resource config.");
    }
    cache = new AlertCache(name);
  }

  @Override
  public Optional<Pair<String, BasicType>> inputFromSubmitter() {
    return Optional.of(new Pair<>("required_services", BasicType.JSON));
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
    Set<String> inputRequiredServices;
    try {
      inputRequiredServices =
          input.map(this::attemptDeserialization).orElse(Collections.emptySet());
    } catch (IllegalArgumentException e) {
      return ConsumableResourceResponse.error(
          "Error reading required_services input from "
              + "submitter. This should be a JSON list of strings.");
    }

    final var isInhibited =
        cache
            .get()
            .flatMap(
                alert ->
                    alert.matches(
                        environment,
                        Stream.concat(
                            Stream.concat(
                                inputRequiredServices.stream(), fixedVidarrNames.stream()),
                            Stream.of(workflowName)),
                        labels))
            .collect(Collectors.toSet());
    if (isInhibited.isEmpty()) {
      return ConsumableResourceResponse.AVAILABLE;
    } else {
      return ConsumableResourceResponse.error(
          String.format(
              "Workflow %s has been auto-inhibited " + "by alert(s) on: %s",
              workflowName, String.join(", ", isInhibited)));
    }
  }

  private class AlertCache extends ValueCache<Stream<AlertDto>> {
    public AlertCache(String name) {
      super("alertmanager " + name, 5, ReplacingRecord::new);
    }

    protected Stream<AlertDto> fetch(Instant lastUpdated) throws Exception {
      if (alertManagerUrl == null) {
        return Stream.empty();
      }
      var response =
          HTTP_CLIENT.send(
              HttpRequest.newBuilder(URI.create(String.format("%s/api/v1/alerts", alertManagerUrl)))
                  .GET()
                  .build(),
              new JsonBodyHandler<>(MAPPER, AlertResultDto.class));
      final var result = response.body().get();
      if (result == null || result.getData() == null) {
        return Stream.empty();
      }
      return result.getData().stream();
    }
  }
}
