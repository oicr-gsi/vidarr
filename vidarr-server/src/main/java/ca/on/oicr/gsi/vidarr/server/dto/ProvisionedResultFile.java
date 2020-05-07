package ca.on.oicr.gsi.vidarr.server.dto;

import java.util.List;

public final class ProvisionedResultFile extends ProvisionedResult {
  private String hash;
  private List<SignedLimsKey> limsKeys;
  private String metatype;
  private String path;
  private long size;

  public String getHash() {
    return hash;
  }

  public List<SignedLimsKey> getLimsKeys() {
    return limsKeys;
  }

  public String getMetatype() {
    return metatype;
  }

  public String getPath() {
    return path;
  }

  public long getSize() {
    return size;
  }

  public void setHash(String hash) {
    this.hash = hash;
  }

  public void setLimsKeys(List<SignedLimsKey> limsKeys) {
    this.limsKeys = limsKeys;
  }

  public void setMetatype(String metatype) {
    this.metatype = metatype;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public void setSize(long size) {
    this.size = size;
  }
}
