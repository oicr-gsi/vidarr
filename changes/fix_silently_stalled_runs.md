Workflow runs no longer stall silently when an operation step throws an unexpected exception
* Steps run from HTTP completion callbacks and from tasks on Vidarr's executor, and nothing observed
  the outcome of either, so an exception thrown by a step, a plugin or a phase transition was lost
  and the run waited forever for a callback that would never come. Such a failure now fails the
  operation and reports the exception with its stack trace.
* An HTTP response code that Vidarr does not recognise, such as the 502 or 503 a busy Cromwell can
  return, now fails the operation (and can be retried) rather than throwing.
* A response that reattempting cannot improve on now fails the operation immediately instead of
  using up the whole retry budget first. That covers redirects, whose target cannot be followed
  because the request URL comes from plugin configuration, and 401 and 403, whose credentials come
  from the same place. Other codes stay retryable, so a plugin that polls until a workflow appears
  keeps working.
* Failures that previously reported a null message, such as a `NullPointerException`, now report the
  exception type. A failure that arrives wrapped by `CompletableFuture` reports the underlying
  exception rather than the wrapper.
* A workflow whose target no longer has a provisioner for one of its output formats now says so,
  rather than failing with an unexplained `NullPointerException`.
