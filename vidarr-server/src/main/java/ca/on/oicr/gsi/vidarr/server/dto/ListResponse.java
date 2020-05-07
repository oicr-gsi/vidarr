package ca.on.oicr.gsi.vidarr.server.dto;

import java.time.ZonedDateTime;
import java.util.List;

public class ListResponse {
  private boolean hasMore;
  private ZonedDateTime next;
  private List<WorkflowRun> runs;
  private long server;

  public ZonedDateTime getNext() {
    return next;
  }

  public List<WorkflowRun> getRuns() {
    return runs;
  }

  public long getServer() {
    return server;
  }

  public boolean isHasMore() {
    return hasMore;
  }

  public void setHasMore(boolean hasMore) {
    this.hasMore = hasMore;
  }

  public void setNext(ZonedDateTime next) {
    this.next = next;
  }

  public void setRuns(List<WorkflowRun> runs) {
    this.runs = runs;
  }

  public void setServer(long server) {
    this.server = server;
  }
}
