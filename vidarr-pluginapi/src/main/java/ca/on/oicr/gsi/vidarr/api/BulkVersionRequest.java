package ca.on.oicr.gsi.vidarr.api;

import java.util.List;

public final class BulkVersionRequest {

  String newVersionKey;
  String oldVersionKey;
  String provider;
  List<BulkVersionUpdate> updates;

  public String getNewVersionKey() {
    return newVersionKey;
  }

  public String getOldVersionKey() {
    return oldVersionKey;
  }

  public String getProvider() {
    return provider;
  }

  public List<BulkVersionUpdate> getUpdates() {
    return updates;
  }

  public void setNewVersionKey(String newVersionKey) {
    this.newVersionKey = newVersionKey;
  }

  public void setOldVersionKey(String oldVersionKey) {
    this.oldVersionKey = oldVersionKey;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public void setUpdates(List<BulkVersionUpdate> updates) {
    this.updates = updates;
  }
}
