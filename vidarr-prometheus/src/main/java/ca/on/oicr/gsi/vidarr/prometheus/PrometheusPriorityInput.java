package ca.on.oicr.gsi.vidarr.prometheus;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.PriorityInput;
import ca.on.oicr.gsi.vidarr.PriorityInputProvider;
import com.fasterxml.jackson.databind.JsonNode;
import io.undertow.server.HttpHandler;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public final class PrometheusPriorityInput implements PriorityInput {
  private static boolean checkLabel(Map<String, String> metric, String label, String value) {
    if (label == null) {
      return true;
    }
    final var entry = metric.get(label);
    return entry != null && entry.equals(value);
  }

  public static PriorityInputProvider provider() {
    return () -> Stream.of(new Pair<>("prometheus", PrometheusPriorityInput.class));
  }

  private VectorCache cache;
  private Integer cacheRequestTimeout;
  private Integer cacheTtl;
  private int defaultPriority;
  private List<String> labels;
  private String query;
  private String url;
  private String workflowNameLabel;
  private String workflowVersionLabel;

  @Override
  public int compute(String workflowName, String workflowVersion, Instant created, JsonNode input) {
    return cache
        .get()
        .filter(
            result ->
                labels.stream()
                        .allMatch(
                            label -> {
                              final var entry = result.getMetric().get(label);
                              return entry != null && entry.equals(input.get(label).asText());
                            })
                    && checkLabel(result.getMetric(), workflowNameLabel, workflowName)
                    && checkLabel(result.getMetric(), workflowVersionLabel, workflowVersion))
        .flatMap(result -> result.getValue().stream())
        .mapToInt(Float::intValue)
        .findFirst()
        .orElse(defaultPriority);
  }

  public int getDefaultPriority() {
    return defaultPriority;
  }

  public List<String> getLabels() {
    return labels;
  }

  public String getQuery() {
    return query;
  }

  public String getUrl() {
    return url;
  }

  public String getWorkflowNameLabel() {
    return workflowNameLabel;
  }

  public String getWorkflowVersionLabel() {
    return workflowVersionLabel;
  }

  @Override
  public Optional<HttpHandler> httpHandler() {
    return Optional.empty();
  }

  @Override
  public BasicType inputFromSubmitter() {
    return BasicType.object(labels.stream().map(label -> new Pair<>(label, BasicType.STRING)));
  }

  public void setCacheRequestTimeout(Integer cacheRequestTimeout) {
    this.cacheRequestTimeout = cacheRequestTimeout;
  }

  public void setCacheTtl(Integer cacheTtl) {
    this.cacheTtl = cacheTtl;
  }

  public void setDefaultPriority(int defaultPriority) {
    this.defaultPriority = defaultPriority;
  }

  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setWorkflowNameLabel(String workflowNameLabel) {
    this.workflowNameLabel = workflowNameLabel;
  }

  public void setWorkflowVersionLabel(String workflowVersionLabel) {
    this.workflowVersionLabel = workflowVersionLabel;
  }

  @Override
  public void startup(String resourceName, String inputName) {
    cache =
        new VectorCache(resourceName + " " + inputName, url, query, cacheRequestTimeout, cacheTtl);
  }
}
