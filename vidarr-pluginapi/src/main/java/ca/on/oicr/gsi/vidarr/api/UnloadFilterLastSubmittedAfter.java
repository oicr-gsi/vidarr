package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.vidarr.UnloadFilter;
import java.time.ZonedDateTime;

public final class UnloadFilterLastSubmittedAfter implements UnloadFilter {

  private ZonedDateTime time;

  @Override
  public <T> T convert(Visitor<T> visitor) {
    return visitor.lastSubmittedAfter(time.toInstant());
  }

  public ZonedDateTime getTime() {
    return time;
  }

  public void setTime(ZonedDateTime time) {
    this.time = time;
  }
}
