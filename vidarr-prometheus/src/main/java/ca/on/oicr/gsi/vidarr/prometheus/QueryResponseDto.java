package ca.on.oicr.gsi.vidarr.prometheus;

public final class QueryResponseDto {

  private VectorDataDto data;
  private String status;

  public VectorDataDto getData() {
    return data;
  }

  public String getStatus() {
    return status;
  }

  public void setData(VectorDataDto data) {
    this.data = data;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
