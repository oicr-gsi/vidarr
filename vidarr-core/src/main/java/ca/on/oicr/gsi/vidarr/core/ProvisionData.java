package ca.on.oicr.gsi.vidarr.core;

import java.util.Map;
import java.util.Set;

/** The accessory data that must be stored during output provisioning */
public final class ProvisionData {
  private Set<? extends ExternalId> ids;
  private Map<String, String> labels;

  public ProvisionData() {
    // Do nothing.
  }

  public ProvisionData(Set<? extends ExternalId> ids) {
    this.ids = ids;
    labels = Map.of();
  }

  public Set<? extends ExternalId> getIds() {
    return ids;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public void setIds(Set<? extends ExternalId> ids) {
    this.ids = ids;
  }

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }
}
