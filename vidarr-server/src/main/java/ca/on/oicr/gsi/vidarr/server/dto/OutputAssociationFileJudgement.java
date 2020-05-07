package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OutputAssociationFileJudgement extends OutputAssociation {
  private String file;
  private List<String> scopes;

  public String getFile() {
    return file;
  }

  public List<String> getScopes() {
    return scopes;
  }

  public void setFile(String file) {
    this.file = file;
  }

  public void setScopes(List<String> scopes) {
    this.scopes = scopes;
  }
}
