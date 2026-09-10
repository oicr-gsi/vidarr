Workflow runs no longer stall silently when an operation step throws an unexpected exception

Note that any current long-running (silently stalled) workflow runs will transition to failed after
this release. Some of these may be retryable once the underlying issue is fixed. The number of
long-running workflow runs (defined as running for more than 2 weeks) can be queried with:

```
SELECT COUNT(*) FROM workflow_run WHERE completed IS NULL AND NOW() - started > INTERVAL '14 days';
```
