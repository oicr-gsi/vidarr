package ca.on.oicr.gsi.vidarr.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

/** A reference to an external key */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExternalId {

  private String id;
  private String provider;

  public ExternalId() {}

  public ExternalId(String provider, String id) {
    this.provider = provider;
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExternalId that = (ExternalId) o;
    return id.equals(that.id) && provider.equals(that.provider);
  }

  public String getId() {
    return id;
  }

  public String getProvider() {
    return provider;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, provider);
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }
}
