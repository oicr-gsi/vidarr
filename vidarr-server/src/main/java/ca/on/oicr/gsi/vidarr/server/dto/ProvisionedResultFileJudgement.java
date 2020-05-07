package ca.on.oicr.gsi.vidarr.server.dto;

public class ProvisionedResultFileJudgement extends ProvisionedResult {

  private String judgementUrl;

  public String getJudgementUrl() {
    return judgementUrl;
  }

  public void setJudgementUrl(String judgementUrl) {
    this.judgementUrl = judgementUrl;
  }
}
