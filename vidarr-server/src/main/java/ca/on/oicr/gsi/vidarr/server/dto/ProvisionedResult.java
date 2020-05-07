package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.ZonedDateTime;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ProvisionedResultFile.class, name = "file"),
  @JsonSubTypes.Type(value = ProvisionedResultFileJudgement.class, name = "filejudgement")
})
public abstract class ProvisionedResult {
  private ZonedDateTime created;
  private ProvisionStatus status;
  private String url;

  ProvisionedResult() {}

  public ZonedDateTime getCreated() {
    return created;
  }

  public ProvisionStatus getStatus() {
    return status;
  }

  public String getUrl() {
    return url;
  }

  public void setCreated(ZonedDateTime created) {
    this.created = created;
  }

  public void setStatus(ProvisionStatus status) {
    this.status = status;
  }

  public void setUrl(String url) {
    this.url = url;
  }
}
