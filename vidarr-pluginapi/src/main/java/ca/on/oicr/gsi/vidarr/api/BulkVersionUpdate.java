package ca.on.oicr.gsi.vidarr.api;

public final class BulkVersionUpdate {

  String add;
  String id;
  String old;

  public String getAdd() {
    return add;
  }

  public String getId() {
    return id;
  }

  public String getOld() {
    return old;
  }

  public void setAdd(String add) {
    this.add = add;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setOld(String old) {
    this.old = old;
  }
}
