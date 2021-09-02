package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A reference to an external key with all versions available */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ExternalMultiVersionKey extends ExternalId {

  private Map<String, Set<String>> versions;

  public ExternalMultiVersionKey(String provider, String id) {
    super(provider, id);
    setVersions(new HashMap<>());
  }

  public ExternalMultiVersionKey(String provider, String id, Map<String, Set<String>> versions) {
    super(provider, id);
    this.versions = versions;
  }

  public Map<String, Set<String>> getVersions() {
    return versions;
  }

  public void setVersions(Map<String, Set<String>> versions) {
    this.versions = versions;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    ExternalKey that = (ExternalKey) o;
    return versions.equals(that.getVersions());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), versions);
  }
}
