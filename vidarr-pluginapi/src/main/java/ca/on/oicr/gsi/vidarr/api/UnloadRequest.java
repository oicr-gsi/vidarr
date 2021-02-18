package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UnloadRequest {
  private UnloadFilter filter;
  private boolean recursive;

  public UnloadFilter getFilter() {
    return filter;
  }

  public boolean isRecursive() {
    return recursive;
  }

  public void setFilter(UnloadFilter filter) {
    this.filter = filter;
  }

  public void setRecursive(boolean recursive) {
    this.recursive = recursive;
  }
}
