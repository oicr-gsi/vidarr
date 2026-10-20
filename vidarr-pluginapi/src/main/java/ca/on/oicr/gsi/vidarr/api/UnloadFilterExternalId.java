package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import ca.on.oicr.gsi.vidarr.UnloadTextSelector;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public final class UnloadFilterExternalId implements UnloadFilter {

  private final UnloadTextSelector id;
  private final String provider;

  @JsonCreator
  public UnloadFilterExternalId(
      @JsonProperty(value = "provider", required = true) String provider,
      @JsonProperty(value = "id", required = true) UnloadTextSelector id) {
    this.provider = Objects.requireNonNull(provider, "provider");
    this.id = Objects.requireNonNull(id, "id");
  }

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
}
