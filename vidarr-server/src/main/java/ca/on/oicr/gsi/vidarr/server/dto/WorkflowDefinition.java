package ca.on.oicr.gsi.vidarr.server.dto;

import java.util.Map;

public final class WorkflowDefinition {
  private String name;
  private String newest;
  private OutputMatchingPolicy outputMatching;
  private String url;
  private Map<String, WorkflowVersionDefinition> versions;

  public String getName() {
    return name;
  }

  public String getNewest() {
    return newest;
  }

  public OutputMatchingPolicy getOutputMatching() {
    return outputMatching;
  }

  public String getUrl() {
    return url;
  }

  public Map<String, WorkflowVersionDefinition> getVersions() {
    return versions;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setNewest(String newest) {
    this.newest = newest;
  }

  public void setOutputMatching(OutputMatchingPolicy outputMatching) {
    this.outputMatching = outputMatching;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setVersions(Map<String, WorkflowVersionDefinition> versions) {
    this.versions = versions;
  }
}
