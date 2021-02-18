package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;

public final class UnloadFilterNot implements UnloadFilter {

  private UnloadFilter filter;

  @Override
  public <T> T convert(Visitor<T> visitor) {
    return visitor.not(filter.convert(visitor));
  }

  public UnloadFilter getFilter() {
    return filter;
  }

  public void setFilter(UnloadFilter filter) {
    this.filter = filter;
  }
}
