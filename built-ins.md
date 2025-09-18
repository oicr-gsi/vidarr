# Built-In Features

These "plugins" come standard with every Víðarr instance and are mostly useful
for manipulating the behaviour of other plugins

## "One of" Input Provisioner

Each input provisioner in a target configuration can only handle one input
format at a time. However, it can still be desirable to use multiple plugins at
once if the user can select which one to use on workflow submission.

    "internal": "FOO",
    "provisioners": {
      "FOO": ...,
      "BAR": ...,
    }
    "type": "oneOf"

This will allow the user to select which provision to use by an algebraic data
type. Any files referenced by a Víðarr ID will use the provisioner specified by
`"internal"`.

This provision will only handle an input format if all the provisioners can
handle that format.

## "One of" Output Provisioner

Each output provisioner in a target configuration can only handle one output
format at a time. However, it can still be desirable to use multiple plugins at
once if the user can select which one to use on workflow submission.

    "provisioners": {
      "FOO": ...,
      "BAR": ...,
    }
    "type": "oneOf"

This will allow the user to select which provision to use by an algebraic data
type.

This provision will only handle an output format if all the provisioners can
handle that format.

## Raw Input Provisioner

The raw input provisioner assumes that output files are available as input
files and passes them through unchanged to workflows.

    "formats": [
      "FILE",
      "DIRECTORY"
    ],
    "type": "raw"

The `"formats"` list determines what input formats it will handle. `"FILE"` and
`"DIRECTORY"` are supported. Note that some workflow engines, including
Cromwell, are not equipped to handle directories in all cases.

## Local Output Provisioner

The Local Output Provisioner moves files from one location in the local environment to another.

    "type": "local"

Only file output types are supported. The provisioner will preserve the original filename of the
outputs.
This Output Provisioner is meant primarily for testing and development, and is not meant for use
in production or to move large (>~2GB) files.

## No-Op Workflow Engine

This Workflow Engine is primarily used during development. Like the Raw Input Provisioner, the
No-Op Workflow Engine passes inputs unchanged through to Output Provisioning.
The No-Op Workflow Engine "supports" all workflow languages, because it completely ignores the
workflow provided.

    "type": "no-op"

## Consumable Resources

Consumable resources provided in Víðarr core.

### Manual Override

Allows overriding a consumable resource to permit workflow runs to run even if they
would hit a limit.

The manual override wraps another consumable resource to allow by-passing its
logic. The `"inner"` property is the configuration for the consumable resource
to wrap. It maintains an allow-list of workflow run IDs that can run even if
the resource would deny them access. The list of allowed IDs is lost on server
shutdown.

```
{
  "inner": { "type": ...},
  "type": "manual-override"
}
```

All the configuration parameters for the inner consumable resource are
unmodified, so this is not a workflow-run visible change. To add or remove
workflow run IDs to the allow list, send an HTTP `POST` or `DELETE` request to
`/consumable-resource/`_name_`/allowed/`_run_ where _name_ is the consumable
resource name and _run_ is the workflow run ID. The current list can be
retrieved by making a `GET` request to
`/consumable-resource/`_name_`/allowed`.

As an example, suppose you wish to have a max-in-flight, but want to run
something urgent. The configuration would look like:

```
"global-max": {
  "inner": {
    "type": "max-in-flight",
    "maximum": 500
  },
  "type": "manual-override"
}
```

And when that urgent deadline happens for a special workflow run:

```
curl -X POST http://vidarr.example.com/api/consumable-resource/global-max/allowed/cbc8ad81b733696d645b42cc08760f4e7c70228a971f4ff2ec1eb0952f18e682
```

### Max-in-Flight

Set a global maximum number of workflow runs that can be simultaneously
active.

```
{
  "maximum": 500,
  "type": "max-in-flight"
}
```

<a id="priority_consumable_resource"></a>

### Priority Consumable Resource

The priority consumable resource operates by computing a number, a priority,
for each workflow run and then allowing the workflow run to proceed
based on that number.

The resource first takes data from the submission request and then
implementations of `PriorityInput` consume this data and produce a numeric
value. Those values are then consumed by `PriorityFormula` to produce a final
definitive score from all the numbers. If a default priority is provided, the
submission request can contain no information and the inputs and formula will
be skipped and the default priority will be used instead.

The priority is then scored by a `PriorityScorer` which determines if the
workflow is allowed to run or not.

See the other sections for the possible inputs, formulas, and scorers.

```
{
  "type": "priority",
  "defaultPriority": null,
  "inputs": {
    "foo": ...,
    "bar": ...
  },
  "formula": ...,
  "scorer": ...
}
```

## Input Provisioners

Input provisioners provided in Víðarr core.

### One-Of

Allows selecting multiple different input provisioners depending on a `"type"`
provided in the metadata.

```
{
  "type": "oneOf",
  "provisioners": {
    "name1": {...},
    "name2": {...},
  }
}
```

### Raw

Allows input to be provided as a string that is assumed to be a path.

```
{
  "type": "raw",
  "format": [ "FILE", "DIRECTORY" ]
}
```

This can be limited to a particular input type format.

## Output Provisioner

Output provisioners provided in Víðarr core.

### One-Of

Allows selecting multiple different output provisioners depending on a `"type"`
provided in the metadata.

```
{
  "type": "oneOf",
  "provisioners": {
    "name1": {...},
    "name2": {...},
  }
}
```

## Priority Input

Priority inputs provided in Víðarr core.

### JSON Array

Takes input as an index into an array and returns the value in that array. If
the index is less than zero, `"underflowPriority"` is returned. If the index is
beyond the end of the array, `"overflowPriority"` is used. The priorities are
stored in `"file"` which must be a JSON file containing an array of integers.

```
{
  "type": "json-array",
  "file": "/path/to/list.json"
  "overflowPriority": 0,
  "underflowPriority": 1000
}
```

### JSON Dictionary

Takes input as a string and looks up the value of that in a dictionary. If
the input is not in the dictionary, `"defaultPriority"` is used. The priorities
are stored in `"file"` which must be a JSON object where all the values are
integers.

```
{
  "type": "json-dictionary",
  "defaultPriority": 0,
  "file": "/path/to/obj.json"
}
```

### One-Of

Allows the submitter to select one of multiple priority inputs using a tagged
union.

```
{
  "type": "oneOf",
  "defaultPriority": 0,
  "inputs": {
    "FOO": {...},
    "BAR": {...}
  }
}
```

The input will take a tagged union/algebraic data type with the appropriate
inputs. If the name provided by the submitter does not match one of the inputs,
`"defaultPriority"` is used instead. The names of the keys of `"inputs"` should
be capitalized for compatibility with Shesmu.

### Prometheus Input

Reads a variable from Prometheus, filtering on the label set, and returns the
current value.

```
{
  "type": "prometheus",
  "cacheRequestTimeout": 1,
  "cacheTtl": 15,
  "defaultPriority": 0,
  "labels": ["bob"],
  "query": "some_prometheus_variable",
  "url": "http://prometheus.example.com:9090",
  "workflowNameLabel": "workflow",
  "workflowVersionLabel": null
}
```

The process this input provider uses is as follows:

1. Execute `"query"` on the Prometheus instance at `"url"`. The query can be
   any valid Prometheus query. If it takes longer than `"cacheRequestTimeout"`
   minutes, then the query will be treated as a failure. The results will be
   cached for `"cacheTtl"` minutes before being refreshed.
2. The submission request will be processed into a label set as described below.
3. All the records that were returned by the query are scanned for a matching
   label set.
4. If a matching label set is found, the last recorded value will be used,
   regardless of when Prometheus observed it.
5. If no matching label set is found, `"defaultPriority"` will be used.

The label set is constructed from the submission request. For each string in
`"labels"`, the submitter must provide a string value. These labels and values
will be used as the label set. For example, with the configuration `"labels":
["bob"]`, the submission request could have `{"bob": "eggs"}` and the filtered
label set would look like `[bob=eggs]`. Additionally, special labels are
available for the workflow name and version. If `"workflowNameLabel":
"workflow"` and the submission request was for `bcl2fastq`, then the label set
would be `[workflow=bcl2fastq]`. This can be further refined with a workflow
version using `"workflowVersionLabel"`, which will only be used if
`"workflowNameLabel"` is not null. Both of these can be turned off by being set
to null.

### Raw Priority Input

Takes an optional integer from the submission request and returns it raw, or
`"defaultPriority"` if not provided.

```
{
  "type": "raw",
  "defaultPriority": 0
}
```

### Remote Input

Takes an arbitrary JSON value and sends it to remote HTTP endpoint for
evaluation. That endpoint must return a single number. The result will be
cached. The `"schema"` is a standard Víðarr type that should be requested from
the submission request.

```
{
  "type": "remote",
  "defaultPriority": 0,
  "schema": "string",
  "ttl": 15,
  "url": "http://foo.com/api/get-priority"
}
```

The `"schema"` property defines a type, including an object types, that will be
required on submission. The data provided by the submission will be sent via
`POST` request as the body to the URL provided. The endpoint must respond with
an integer for the priority or null to use the default priority. The result
will be cached for `"ttl"` minutes before being reattempted.

### Tuple-Wrapping Input

This changes the type of an input provider for compatibility with Shesmu. The
crux is this: Shesmu's tagged unions are more limited than Víðarr's. Shesmu
requires that a tagged union have a tuple or object while Víðarr permits either
of those. When using the _one-of_ input source, this introduces the possibility
of creating a type that Shesmu cannot process. This allows wrapping an
priority input's type in a single element tuple, thereby making it compatible
with Shesmu.

```
{
  "type": "tuple",
  "inner": {...}
}
```

## Priority Formula

Priority formulas provided in Víðarr core.

### Constant

Returns a constant value.

```
{
  "type": "constant",
  "value": 100
}
```

# Input Variable

Accesses one of the input scores. If no input score has the identifier
`"name"`, the minimum integer value is used.

```
{
  "type": "input",
  "name: "foo"
}
```

### Minimum and Maximum

Takes the minimum or maximum of other formulas.

```
{
  "type": "maximum",
  "components": [ ... ]
}
```

or

```
{
  "type": "minimum",
  "components": [ ... ]
}
```

### Product

Computes the product of other formulas (_i.e._, multiplies their scores).

```
{
  "type": "product",
  "components": [ ... ]
}
```

### Subtraction

Computes the difference between two formulas; the result of `"left"` minus the
result of `"right"`.

```
{
  "type": "difference",
  "left": ...,
  "right": ...
}
```

### Summation

Computes the summation of other formulas (_i.e._, adds their scores).

```
{
  "type": "sum",
  "components": [ ... ]
}
```

# Temporal Escalating with Multiplier

Increases the priority as a workflow run sits around. The duration the workflow
run has been waiting is looked up in the `"escalation"` object; the keys are an
[ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations) and the
values are a floating point number. The smallest matching duration is used and
the score is multiplied by the value provided. Values need to be greater than 1
to increase priority. If workflow run has been waiting less than the smallest
duration in the dictionary, the original priority is used. The original
priority is provided using the `"base"` formula.

```
{
  "type": "escalating-multiplier",
  "base": ...,
  "escalation": {
    "PT1H": 1.2,
    "PT12H": 2.0
  }
}
```

# Temporal Escalating with Offset

Increases the priority as a workflow run sits around. The duration the workflow
run has been waiting is looked up in the `"escalation"` object; the keys are an
[ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations) and the
values are a integer. The smallest matching duration is used and the value
provided is added to the original score. Values need to be greater than 1 to
increase priority. If workflow run has been waiting less than the smallest
duration in the dictionary, the original priority is used. The original
priority is provided using the `"base"` formula.

```
{
  "type": "escalating-offset",
  "base": ...,
  "escalation": {
    "PT1H": 10,
    "PT12H": 100
  }
}
```

## Priority Scorer

Priority scorers provided in Víðarr core.

### All Of

Checks several priority scorers and allows permits the workflow run to proceed
if all scorers allow it to proceed.

```
{
  "type": "all",
  "scorers": [ ... ]
}
```

This can be combined with the ranked max-in-flight family to allow a global
limit with per-workflow limits. For example:

```
{
  "scorers": [
    {
      "maxInFlight": 500,
      "type": "ranked-max-in-flight",
      "hogFactor": 3
    },
    {
      "maxInFlight": 20,
      "useCustom": true,
      "type": "ranked-max-in-flight-by-workflow"
    }
  ],
  "type": "all"
}
```

This would let the top 500 workflow runs to execute as long as they are also
among the top 20 workflow run in their respective workflow type.

### Any Of

Checks several priority scorers and allows permits the workflow run to proceed
if any scorer would allow it to proceed.

```
{
  "type": "any",
  "scorers": [ ... ]
}
```

### Cut-off

Allows the workflow run to start if the score is strictly greater than `"cutoff"`.

```
{
  "type": "cutoff",
  "cutoff": 9000
}
```

### Ranked Max-in-flight

Ranks workflow runs by score and allows the top ones to run, where the number
allowed to run is `"maxInFlight"`. This workflow makes a best effort to keep
the total number running at or below that limit, but various conditions,
including server relaunch or being used in an `"any"` scorer, may cause it to
exceed that bound.

This scorer comes in a few flavours:

- `"ranked-max-in-flight"`: the limit is applied to all workflow runs
- `"ranked-max-in-flight-by-workflow"`: the limit is applied per workflow type
- `"ranked-max-in-flight-by-workflow-version"`: the limit is applied per workflow type
  including version

The limit cannot be set individually per workflow in this configuration.
However, `"ranked-max-in-flight-by-workflow"` and
`"ranked-max-in-flight-by-workflow-version"` have an additional property
`"useCustom"`, which will use the max-in-flight values set when a workflow is
created, as is visible through the `/api/max-in-flight` endpoint. In that case
`"maxInFlight"` is treated as a fallback.

`"ranked-max-in-flight"` has an additional property `"hogFactor"`, which is defined as the maximum
number of unique workflows that may occupy the priority queue before the scorer is forced to allow
a lower-priority workflow to run. This is meant to provide some relief when one or a few workflows
with a low by-workflow max-in-flight and a high number of waiting workflow runs cause the
ranked-max-in-flight to become underutilized. Values of 0 or lower disable this feature.

```
{
  "type": "ranked-max-in-flight",
  "maxInFlight": 500,
  "hogFactor": 3
}
```

or

```
{
  "type": "ranked-max-in-flight-by-workflow",
  "useCustom": true,
  "maxInFlight": 50
}
```

or

```
{
  "type": "ranked-max-in-flight-by-workflow",
  "useCustom": false,
  "maxInFlight": 50
}
```

### Resource-Optimizing

The `"resource-optimizing"` priority scorer operates like a combination of ranked-max-in-flight and
ranked-max-in-flight-by-workflow-version. It trends overall towards launching workflow runs by
their priority, however when a workflow reaches its max-in-flight, its waiting workflow runs are
deprioritized in order to let a workflow with room to run launch its highest-priority workflow runs.
Therefore, workflow runs with an overall lower priority may launch ahead of workflow runs with an
overall high priority if the latter has reached its max-in-flight-by-workflow-version.

The property `"useCustom"` will cause the scorer to use the max-in-flight values set when a workflow
is created for its max-in-flight-by-workflow-version if true. If false, all workflow versions will
be given the max-in-flight set in `"maxInFlightPerWorkflow"`.

```
{
  "type": "resource-optimizing",
  "useCustom": true,
  "globalMaxInFlight": 500,
  "maxInFlightPerWorkflow": 50
}
```