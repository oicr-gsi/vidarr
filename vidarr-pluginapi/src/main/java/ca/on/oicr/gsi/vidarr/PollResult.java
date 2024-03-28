package ca.on.oicr.gsi.vidarr;

import java.time.Duration;

/** Indicate how a {@link OperationStatefulStep#poll(Duration)} step should proceed */
public abstract sealed class PollResult
    permits PollResultActive, PollResultFailed, PollResultFinished {

  /** Consumes a poll result */
  public interface Visitor {

    /**
     * Indicates the task is ongoing.
     *
     * @param status the current status associated with the task
     */
    void active(WorkingStatus status);

    /**
     * Indicates the task has failed.
     *
     * @param error an error associated with the task
     */
    void failed(String error);

    /** Indicates the task has finished successfully. */
    void finished();
  }

  /**
   * Indicate that the task is still ongoing and will need to be polled again
   *
   * @param status the current status of the task, for setting the Vidarr status
   * @return the indicator to the poll task
   */
  public static PollResult active(WorkingStatus status) {
    return new PollResultActive(status);
  }

  /**
   * Indicate that the task has failed and the steps need to enter an error state
   *
   * @param error the error message to provide
   * @return the indicator to the poll task
   */
  public static PollResult failed(String error) {
    return new PollResultFailed(error);
  }

  /**
   * Indicate that the task has finished successfully and the next step should be run
   *
   * @return the indicator to the poll task
   */
  public static PollResult finished() {
    return new PollResultFinished();
  }

  /**
   * Check which state the result is in
   *
   * @param visitor a consumer of the result
   */
  public abstract void visit(Visitor visitor);
}
