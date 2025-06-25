package ca.on.oicr.gsi.vidarr.api;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ReprovisionOutRequest{
  private List<String> analysisHashIds;
  private String outputProvisionerName;

  public List<String> getAnalysisHashIds() {
    return analysisHashIds;
  }

  public String getOutputProvisionerName() {
    return outputProvisionerName;
  }

  public void setAnalysisHashIds(List<String> analysisHashIds) {
    this.analysisHashIds = analysisHashIds;
  }

  public void setOutputProvisionerName(String outputProvisionerName) {
    this.outputProvisionerName = outputProvisionerName;
  }
}
