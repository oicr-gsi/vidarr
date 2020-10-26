package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AnalysisProvenanceRequest {
  private Set<AnalysisType> analysisTypes = Collections.singleton(AnalysisType.FILE);
  private long epoch;
  private boolean includeParameters;
  private long timestamp;
  private VersionPolicy versionPolicy = VersionPolicy.NONE;
  private Set<String> versionTypes = Collections.emptySet();

  public Set<AnalysisType> getAnalysisTypes() {
    return analysisTypes;
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

  public void setAnalysisTypes(Set<AnalysisType> analysisTypes) {
    this.analysisTypes = analysisTypes;
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
