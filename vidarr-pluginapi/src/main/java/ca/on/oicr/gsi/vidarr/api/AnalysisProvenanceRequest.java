package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AnalysisProvenanceRequest {
  private Set<AnalysisOutputType> analysisOutputTypes =
      Collections.singleton(AnalysisOutputType.FILE);
  private long epoch;
  private boolean includeParameters;
  private long timestamp;
  private VersionPolicy versionPolicy = VersionPolicy.NONE;
  private Set<String> versionTypes = Collections.emptySet();

  public Set<AnalysisOutputType> getAnalysisTypes() {
    return analysisOutputTypes;
  }

  public long getEpoch() {
    return epoch;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public VersionPolicy getVersionPolicy() {
    return versionPolicy;
  }

  public Set<String> getVersionTypes() {
    return versionTypes;
  }

  public boolean isIncludeParameters() {
    return includeParameters;
  }

  public void setAnalysisTypes(Set<AnalysisOutputType> analysisOutputTypes) {
    this.analysisOutputTypes = analysisOutputTypes;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  public void setIncludeParameters(boolean includeParameters) {
    this.includeParameters = includeParameters;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public void setVersionPolicy(VersionPolicy versionPolicy) {
    this.versionPolicy = versionPolicy;
  }

  public void setVersionTypes(Set<String> versionTypes) {
    this.versionTypes = versionTypes;
  }
}
