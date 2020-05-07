package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = OutputAssociationFiles.class, name = "files"),
  @JsonSubTypes.Type(value = OutputAssociationFileJudgement.class, name = "filejudgement"),
  @JsonSubTypes.Type(value = OutputAssociationList.class, name = "list"),
  @JsonSubTypes.Type(value = OutputAssociationRoot.class, name = "root")
})
public abstract class OutputAssociation {

  OutputAssociation() {}
}
