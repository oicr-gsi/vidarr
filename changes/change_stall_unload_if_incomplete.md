If an unload request selects one or more workflow runs or downstream workflow runs which are incomplete, the unload will fail with an error message informing which workflow runs need to be dealt with before the unload can proceed.

Note that copy-out requests will proceed as usual. It is possible that a copy-out operation could succeed while an unload with the same filter would fail, as copy-out only returns completed workflow runs and unload now checks all downstream workflow runs.
