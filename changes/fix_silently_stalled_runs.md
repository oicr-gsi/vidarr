Workflow runs no longer stall silently when an operation step throws an unexpected exception
* Steps run from HTTP completion callbacks and from tasks on Vidarr's executor, and nothing observed
  the outcome of either, so an exception thrown by a step, a plugin or a phase transition was lost
  and the run waited forever for a callback that would never come. Such a failure now fails the
  operation and reports the exception with its stack trace.
* An HTTP response code that Vidarr does not recognise, such as the 502 or 503 a busy Cromwell can
  return, now fails the operation (and can be retried) rather than throwing.
* Failures that previously reported a null message, such as a `NullPointerException`, now report the
  exception type.
