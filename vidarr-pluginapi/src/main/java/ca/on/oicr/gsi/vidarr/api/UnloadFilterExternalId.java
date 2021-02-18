package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.UnloadTextSelector;

public final class UnloadFilterExternalId implements UnloadFilter {

  private UnloadTextSelector id;
  private String provider;

  @Override
  public <T> T convert(Visitor<T> visitor) {
    return visitor.externalKey(provider, id.stream());
  }

  public UnloadTextSelector getId() {
    return id;
  }

  public String getProvider() {
    return provider;
  }

  public void setId(UnloadTextSelector id) {
    this.id = id;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }
}
