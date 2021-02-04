package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * The workflow runs fetched by an analysis provenance request
 *
 * @param <V> the type of version keys that will be returned; {@link
 *     AnalysisProvenanceRequest#setVersionPolicy(VersionPolicy)} will set the type that should be
 *     used here. See {@link VersionPolicy} for details.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AnalysisProvenanceResponse<V extends ExternalId> {
  private long epoch;
  private List<ProvenanceWorkflowRun<V>> results;
  private long timestamp;

  public long getEpoch() {
    return epoch;
  }

  public List<ProvenanceWorkflowRun<V>> getResults() {
    return results;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  public void setResults(List<ProvenanceWorkflowRun<V>> results) {
    this.results = results;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
