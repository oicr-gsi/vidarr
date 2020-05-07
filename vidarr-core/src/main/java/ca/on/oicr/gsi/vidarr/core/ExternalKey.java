package ca.on.oicr.gsi.vidarr.core;

import java.util.Map;

/** A reference to an external key with version information */
public final class ExternalKey extends ExternalId {

  private Map<String, String> versions;

  public Map<String, String> getVersions() {
    return versions;
  }

  public void setVersions(Map<String, String> versions) {
    this.versions = versions;
  }
}
