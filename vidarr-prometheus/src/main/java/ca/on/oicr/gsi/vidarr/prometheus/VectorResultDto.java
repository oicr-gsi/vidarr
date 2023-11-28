package ca.on.oicr.gsi.vidarr.prometheus;

import java.util.List;
import java.util.Map;

public final class VectorResultDto {

  private Map<String, String> metric;
  private List<Float> value;

  public Map<String, String> getMetric() {
    return metric;
  }

  public List<Float> getValue() {
    return value;
  }

  public void setMetric(Map<String, String> metric) {
    this.metric = metric;
  }

  public void setValue(List<Float> value) {
    this.value = value;
  }
}
