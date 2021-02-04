package ca.on.oicr.gsi.vidarr.api;

/** Set the type of version information return with workflow run provenance */
public enum VersionPolicy {
  /**
   * All known values associated with a version are returned. Use {@link ExternalMultiVersionKey} as
   * the type argument to {@link AnalysisProvenanceResponse}.
   */
  ALL,
  /**
   * Only the newest value associated with a version is returned. Use {@link ExternalKey} as the
   * type argument to {@link AnalysisProvenanceResponse}.
   */
  LATEST,
  /**
   * No version values are returned. Use {@link ExternalId} as the type argument to {@link
   * AnalysisProvenanceResponse}.
   */
  NONE
}
