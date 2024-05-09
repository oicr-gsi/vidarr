# Loading and Unloading Data
It can be desirable to remove successfully completed workflow runs from the
Víðarr database. Some reasons include:

- the workflow run was processed incorrectly and needs to be reprocessed
- the workflow run is no longer useful
- the workflow run needs to be run with a newer version of the workflow

Víðarr has a way to _unload_ workflow runs from the database. These extracts
can be used as archives or _loaded_ into a Víðarr instance (either the same one
or another at a later date). This can be used to split up a Víðarr instance,
move or copy workflow runs between environments, create backups, or migrate
workflow runs from another system into Víðarr.

Workflow runs are selected using a _filter_ and then one of three operations
happens:

- copy-out: the selected workflow runs are dumped in the load/unload format
- copy-out recursive: the selected workflow runs and any workflow runs that
  consumed the output of a selected workflow run are dumped in the load/unload
  format
- unload: the selected workflow runs and any workflow runs that consumed the
	output of a selected workflow run are dumped in the load/unload format and
  *removed* from the database. Any unfinished workflow runs that consume this
  data will enter a failed state (though not be removed directly)

The unload operation is *always* recursive since doing anything else would
leave orphaned records in the database. Unloaded data is written to a file on
the server and the file path is provided in the HTTP response while copy-out
operations are provided in the HTTP response.

A request looks like:

```
{
  "filter": ...,
  "recursive": true
}
```

This can be sent to either the `/api/copy-out` or `/api/unload` endpoints. The
unload endpoint ignores the value of the `"recursive"` property and always
works recursively.

The output format is similar to `/api/provenance` with `"versionPolicy":
"ALL"` in the request. It consists of a JSON object with the following fields:

```
{
  "workflows": [...],
  "workflowVersions": [...],
  "workflowRuns": [...]
}
```

The `"workflows"` are objects with two properties: `"name"`, which is a string,
and `"labels"`, which is an object of label names and the type of the label
values; the labels are in same format as the request to create a new workflow.

The `"workflowVersions"` are objects with a `"name"` and `"version"` property
and all the properties used when adding a new workflow version using the
`/api/`_workflow_`/`_version_ endpoint.

All of the workflow runs in the request must be present in the workflow and
workflow version definitions provided. Extra definitions may be provided. If a
workflow is new to Víðarr, it will be created with max-in-flight at 0.

Unloaded data can then be inserted by pushing this object to `/api/load`. The
workflows will be validated and installed if necessary. If there are any
errors, the entire load will be abandoned.

During the load procedure, all the identifiers are verified. If the contents of
the file have been manipulated, then the identifiers must also be updated. For
information on how identifiers are calculated, see [Víðarr
identifiers](identifiers.md).

An example load file is available in `examples/loadable_workflows.json`.
