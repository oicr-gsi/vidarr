Plugin API: `OperationControlFlow` gains two methods for reporting failures
* `guard(Runnable)` runs a block of work and converts any exception that escapes it into an
  operation error. Any step that resumes work on another thread, whether from an HTTP completion
  callback or a scheduled task, must route that work through it so that a bug fails the operation
  instead of stalling the workflow run.
* `describe(Throwable)` produces a non-null description of a failure, for use in place of
  `getMessage()` when reporting an error.

Both are defaults or statics, so existing plugins continue to compile and run unchanged.
