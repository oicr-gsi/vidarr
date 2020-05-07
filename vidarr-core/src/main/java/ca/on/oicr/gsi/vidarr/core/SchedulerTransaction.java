package ca.on.oicr.gsi.vidarr.core;

/** A transaction so the processor can make updates to the database atomically */
public interface SchedulerTransaction {
  /** Perform all pending updates. */
  void commit();
}
