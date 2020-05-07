package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class OutputAssociationList extends OutputAssociation {

  private List<OutputEntry> entries;

  public List<OutputEntry> getEntries() {
    return entries;
  }

  public void setEntries(List<OutputEntry> entries) {
    this.entries = entries;
  }
}
