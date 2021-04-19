package ca.on.oicr.gsi.vidarr.prometheus;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** Bean containing list of alerting results JSON from Alert Manager */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AlertResultDto {
  private List<AlertDto> data;
  private String status;

  public List<AlertDto> getData() {
    return data;
  }

  public String getStatus() {
    return status;
  }

  public void setData(List<AlertDto> data) {
    this.data = data;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
