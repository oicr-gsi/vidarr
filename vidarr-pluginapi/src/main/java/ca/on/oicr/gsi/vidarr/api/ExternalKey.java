package ca.on.oicr.gsi.vidarr.api;

import java.util.Map;

/** A reference to an external key with version information */
public final class ExternalKey extends ExternalId {

  private Map<String, String> versions;

  public ExternalKey() {}

  public ExternalKey(String provider, String id, Map<String, String> versions) {
    super(provider, id);
    this.versions = versions;
  }

  public Map<String, String> getVersions() {
    return versions;
  }

  public void setVersions(Map<String, String> versions) {
    this.versions = versions;
  }
}
