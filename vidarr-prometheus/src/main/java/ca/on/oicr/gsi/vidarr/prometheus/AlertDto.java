package ca.on.oicr.gsi.vidarr.prometheus;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Bean of the Alert Manager alert JSON object */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertDto {
  private ObjectNode annotations;
  private String endsAt;
  private String fingerprint;
  private String generatorURL;
  private ObjectNode labels;
  private List<String> receivers;
  private String startsAt;
  private ObjectNode status;

  public ObjectNode getAnnotations() {
    return annotations;
  }

  public String getEndsAt() {
    return endsAt;
  }

  public String getFingerprint() {
    return fingerprint;
  }

  public String getGeneratorURL() {
    return generatorURL;
  }

  public ObjectNode getLabels() {
    return labels;
  }

  public List<String> getReceivers() {
    return receivers;
  }

  public String getStartsAt() {
    return startsAt;
  }

  public ObjectNode getStatus() {
    return status;
  }

  private List<String> lb = new ArrayList<>(2);

  /**
   * Check if our alert is of type AutoInhibit that specifies the current Vidarr environment, and
   * one of the given labels matches the alert's labels
   *
   * @param environment the specified environment
   * @param labelsOfInterest the specified label names
   * @return the matching labels
   */
  public Stream<String> matches(
      String alertname,
      String environment,
      Set<String> labelsOfInterest,
      Stream<String> valuesOfInterest) {
    if (!labels.get("alertname").asText("").equals(alertname)) {
      return Stream.empty();
    }
    if (labels.hasNonNull("environment")
        && !labels.get("environment").asText("").equals(environment)) {
      return Stream.empty();
    }

    // Lowercase to guard against bespoke workflow names
    Set<String> targetValues =
        valuesOfInterest.map(v -> v.toLowerCase()).collect(Collectors.toSet());

    return labelsOfInterest.stream()
        .filter(labels::hasNonNull)
        .filter(l -> targetValues.contains(labels.get(l).asText("").toLowerCase()))
        .map(l -> labels.get(l).asText(""));
  }

  public void setAnnotations(ObjectNode annotations) {
    this.annotations = annotations;
  }

  public void setEndsAt(String endsAt) {
    this.endsAt = endsAt;
  }

  public void setFingerprint(String fingerprint) {
    this.fingerprint = fingerprint;
  }

  public void setGeneratorURL(String generatorURL) {
    this.generatorURL = generatorURL;
  }

  public void setLabels(ObjectNode labels) {
    this.labels = labels;
  }

  public void setReceivers(List<String> receivers) {
    this.receivers = receivers;
  }

  public void setStartsAt(String startsAt) {
    this.startsAt = startsAt;
  }

  public void setStatus(ObjectNode status) {
    this.status = status;
  }
}
