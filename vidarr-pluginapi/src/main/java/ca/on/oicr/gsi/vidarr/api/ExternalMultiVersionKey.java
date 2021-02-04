package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.Set;

/** A reference to an external key with all versions available */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ExternalMultiVersionKey extends ExternalId {

  private Map<String, Set<String>> versions;

  public Map<String, Set<String>> getVersions() {
    return versions;
  }

  public void setVersions(Map<String, Set<String>> versions) {
    this.versions = versions;
  }
}
