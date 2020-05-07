package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.ZonedDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ListRequest {
  private long server;
  private ZonedDateTime start;

  public long getServer() {
    return server;
  }

  public ZonedDateTime getStart() {
    return start;
  }

  public void setServer(long server) {
    this.server = server;
  }

  public void setStart(ZonedDateTime start) {
    this.start = start;
  }
}
