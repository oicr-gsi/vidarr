package ca.on.oicr.gsi.vidarr.prometheus;

import java.util.List;

public final class VectorDataDto {

  private List<VectorResultDto> result;
  private String resultType;

  public List<VectorResultDto> getResult() {
    return result;
  }

  public String getResultType() {
    return resultType;
  }

  public void setResult(List<VectorResultDto> result) {
    this.result = result;
  }

  public void setResultType(String resultType) {
    this.resultType = resultType;
  }
}
