package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

/** A reference to an external key with version information */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ExternalKey extends ExternalId {

  @JsonProperty("versions")
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
