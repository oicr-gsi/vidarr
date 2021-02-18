package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.UnloadTextSelector;

public final class UnloadFilterExternalProvider implements UnloadFilter {

  private UnloadTextSelector providers;

  @Override
  public <T> T convert(Visitor<T> visitor) {
    return visitor.externalKey(providers.stream());
  }

  public UnloadTextSelector getProviders() {
    return providers;
  }

  public void setProviders(UnloadTextSelector providers) {
    this.providers = providers;
  }
}
