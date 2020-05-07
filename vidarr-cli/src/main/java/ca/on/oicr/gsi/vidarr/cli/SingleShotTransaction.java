package ca.on.oicr.gsi.vidarr.cli;

import ca.on.oicr.gsi.vidarr.core.SchedulerTransaction;

final class SingleShotTransaction implements SchedulerTransaction {
  @Override
  public void commit() {
    // Do nothing.
  }
}
