# Víðarr Plugin Developer's Guide

Víðarr uses plugins to allow interaction with a diverse set of systems. Plugins
are loaded by the Java
[`ServiceLoader`](https://docs.oracle.com/javase/9/docs/api/java/util/ServiceLoader.html)
and exported by the [Java module
system](https://www.oracle.com/corporate/features/understanding-java-9-modules.html)
using the `provides` keyword. All plugins need to depend only on the
`ca.on.oicr.gsi.vidarr.pluginapi` module to hook into the Víðarr
infrastructure.

There are several services that a plugin can provide and a plugin is free to
provide multiple. Plugins are loaded from JSON data in the Víðarr configuration
file or, in the case of an unload filter, user requests, using Jackson. Each
plugin can load whatever Jackson-compatible data from JSON it requires. Each
plugin has a small "provider" class which provides type information for
Jackson. In the JSON file, the `"type"` attribute will be used to create the
appropriate class instance. The provider class lists what values for `"type"`
correspond to what Java objects that Jackson should load. Since objects are
instantiated by Jackson, most have a `startup` method that is called after
loading is complete where the plugin can do any initialisation required. If it
throws exceptions, the Víðarr server will fail to start, which is probably the
correct behaviour for a badly misconfigured plugin.

As an example of a configuration file:

```
"consumableResources": {
  "total": {
    "maximum": 500,
    "type": "max-in-flight"
  }
}
```

The `"type": "max-in-flight"` property is used to connect this configuration to
`ca.on.oicr.gsi.vidarr.core.MaxInFlightConsumableResource`. The `"maximum"`
property is populated by Jackson into an instance of that class. Here `"total"`
is an arbitrary name set by the server administrator they will use in the
`"targets"` section of the main configuration file.

These are high-level overviews of the purpose and general constraints for each
service. The JavaDoc for each interface provides the details for how the
interfaces should behave.

Additionally, plugins communicate with the outside world through the types they
expect. A description of the types is provided in
`ca.on.oicr.gsi.vidarr.SimpleType` and the format for the values is meant to be
compatible with Shesmu's.

Plugins are meant to run asynchronously. Most plugins are given a `WorkMonitor`
instance which allows a plugin to communicate back to Víðarr and schedule
future asynchronous tasks. Plugins must implement recovery from crash, so are
expected to journal their current state to the database. The `WorkMonitor`
provides methods to journal state to the database for crash recovery and to
provide status information to users.

Most plugins have a `recover` method. If Víðarr is restarted, the plugin will
be asked to recover its state from the last state information in journaled to
the database using the `WorkMonitor`. Plugins are expected to be able to pick
up where they left off based only on this information.

See [Víðarr Code Style](code-style.md) for preferred code formatting.

# Consumable Resource

Consumable resources implement
`ca.on.oicr.gsi.vidarr.ConsumableResourceProvider` and
`ca.on.oicr.gsi.vidarr.ConsumableResource`. These plugins are responsible for
delaying workflow run execution until resources are available.

The plugins can be associated with targets in the server configuration.
Consumable resources _may_ request that submitters provide information or
operate on the existence of a workflow run. Consumable resources is a broad
term for anything that can be used to delay a workflow run from launching. Some
of them are "quota"-type resources (such as RAM, disk, max-in-flight) where the
resources must be available at the start of its run and it holds the resource
until the workflow completes (successfully or not), at which point the resource
may be reused by another workflow run. Within quota-type, some require
information (_e.g._, the amount of RAM), while others are based purely on the
existence of the workflow run (_e.g._, max-in-flight). The priority consumable
resources operates within the restrictions imposed from quota resources and allows
users to manually set the order in which workflow runs will launch. Other
resource are more "throttling"-type. These include maintenance schedules and
Prometheus alerts which block workflow runs from starting but don't track
anything once the workflow run is underway.

Consumable resources are long-running. Whenever Víðarr attempts to run a
workflow, it will consult the consumable resources to see if there is capacity
to run the workflow (the `request` method). At that point the consumable
resource must make a decision as to whether the workflow can proceed. Once the
workflow has finished running (successfully or not), Víðarr will `release` the
resource so that it can be used again. When Víðarr restarts, any running
workflows will be called with `recover` to indicate that the resource is being
used and the resource cannot stop the workflow even if the resource is
over-capacity.

Consumable resources can request data from the user, if desired. The
`inputFromSubmitter` can return an empty optional to indicate that no
information is required or can indicate the name and type of information that
is required. The `request` and `release` methods will contain a copy of this information,
encoded as JSON, if the submitter provided it. The JSON data has been
type-checked by Víðarr, so it should be safe to convert to the expected type
using Jackson.

Sometimes, consumable resources are doing scoring that would be helpful to know
for debugging purposes. In that case, the resource can return a custom
`ConsumableResourceResponse` that uses the `Visitor.set` method to export
numeric statistics that will be available in the `"tracing"` property. Víðarr
will prefix these variables with the consumable resource's name.

# Input Provisioners

Input provisioners implement `ca.on.oicr.gsi.vidarr.InputProvisionerProvider`
and `ca.on.oicr.gsi.vidarr.InputProvisioner`. These plugins are responsible for
taking files from existing workflows or provided by the user and generating a
file path that a workflow can use. Input provisioners can choose to handle only
some kinds of input data (files vs directories) and the system administrator
can choose multiple provisoners to handle both.

This plugin and the workflow plugins must have a mutual understanding of what a
file path means. That is somewhat the responsibility of the system
administrator. For instance, if in an HPC environment with shared disk, the
system administrator must direct the input provisioner plugin to write to a
shared directory instead of, say, `/tmp` and ensure the right permissions are
set up.
These are not the responsibility of the plugin author.

This plugin type uses the [operations API](#operations-api).

# Output Provisioners

Output provisioners implement `ca.on.oicr.gsi.vidarr.OutputProvisionerProvider`
and `ca.on.oicr.gsi.vidarr.OutputProvisioner`. These plugins are responsible
for taking data (files or JSON values) from completed workflows, moving the
data into permanent storage and writing back a file path or URL that will be
associated with the correct external identifiers. Output provisioners can
choose to handle only some kinds of output data (files, logs, data-warehouse
entries, or QC judgements) and the system administrator can choose multiple
provisioners to handle all the input types they require.

Output provisioners are run twice for each workflow: a preflight and a
provision out. The preflight is run before the workflow has started and allows
the plugin to validate any configuration metadata provided by the submitter
(_i.e._, Shesmu) to check it for validity. Once the workflow is completed, the
provision out step will be run with the metadata provided by the submitter and
the output provided by the workflow.

This plugin type uses the [operations API](#operations-api).

# Runtime Provisioners

Runtime provisioners implement `ca.on.oicr.gsi.vidarr.RuntimeProvisionerProvider`
and `ca.on.oicr.gsi.vidarr.RuntimeProvisioner`. These plugins are responsible for
extracting non-specific output from a workflow. While output provisioners are
fed specific data from a workflow (_e.g._, output file), runtime provisioners
operate on the workflow run as a whole. They can provision out information such
as performance metrics, workflow run logs, or machine statistics.

This plugin and the workflow plugins must have a mutual understanding of what a
workflow engine's identifier means. That is somewhat the responsibility of the
system administrator.

This plugin type uses the [operations API](#operations-api).

# Workflow Engine

Workflow engines implement `ca.on.oicr.gsi.vidarr.WorkflowEngineProvider`
and `ca.on.oicr.gsi.vidarr.WorkflowEngine`. These plugins are responsible for
running workflows and collecting the output from the workflow. A workflow
engine can support multiple languages (see
`ca.on.oicr.gsi.vidarr.WorkflowLanguage` for a complete list) and indicates
which ones are allowed via the `supports` method.

The workflow engine will be given the complete input to the workflow (with real
paths provided by the input provisioners) and the workflow itself. Once the
workflow has completed, it must provide a JSON structure that references the
output of the workflow. Víðarr will identify the output files generated by the
workflow engine and they will be passed to the output provisioners.

After the output provisioners have completed, the workflow engine will be
called again to cleanup any output, if this is appropriate. If the workflow
engine does not support cleanup, it should gracefully succeed during the
clean-up (and clean-up recovery) methods.

This plugin type uses the [operations API](#operations-api).

# Unload Filters

Unload filters implement `ca.on.oicr.gsi.vidarr.UnloadFilterProvider` and
`ca.on.oicr.gsi.vidarr.UnloadFilter`. These plugins allow customisable
selection of workflows for unloading. For an understanding of unloading
filters, see [Loading and Unloading](load-unload.md).

The user will specify a filter as a JSON object with a `"type"` property. The
classes implementing `UnloadFilter` will be deserialized by Jackson. The
`UnloadFilterProvider` associates the strings used in `"type"` to the objects.
One provider can provide multiple filter types. The types used should be
_plugin_`-`_filter_; names without dashes and names starting with `vidarr-` are
reserved by Víðarr. The _filter_ can have dashes in it if desired. If two
plugins provide duplicate type names, Víðarr will fail to load.

Once a filter is deserialised, it needs to convert the request into a query
Víðarr can apply to its database. That is, it needs to be converted to a query
made of only workflow, workflow run, and external key matches. The server will
call `convert` with an `UnloadFilter.Visitor` so that the filter can determine
whatever information it needs and generate an output query.

For example, if external keys are connected to Pinery, then a filter might want
to filter on runs. A filter could query Pinery and get all the external
identifiers associated with that run and then construct a query based on those
to match workflow runs that use any of those identifiers.

# Priority Consumable Resource Inputs, Formulas, and Scorers

The priority consumable resource takes plugins for the inputs, formulas, and
scorers. These are used by the [Priority Consumable
Resource](#priority_consumable_resource). This follows the same pattern as the
other plugins: an implementation of `ca.on.oicr.gsi.vidarr.PriorityInput`,
`ca.on.oicr.gsi.vidarr.PriorityFormula`, or
`ca.on.oicr.gsi.vidarr.PriorityScorer` for the input, formula, and scorers,
respectively and there needs to be a corresponding implementation of
`PriorityFormulaProvider`, `PriorityInputProvider`, or
`PriorityScorerProvider`.

In the priority consumable resource's configuration, the `"type"` property will
select the appropriate input, formula, or scorer and deserialize it as a JSON
object.

Each component will be called for every pending workflow run, so the analysis
should be relatively fast. `PriorityInput` implementations should cache results
from external services.

<!-- TODO: GP-4801 Delete this notice -->
Please note that priority formaulae will not be applied when falling back on a default
priority input value.

<a id="operations-api"></a>

# The Operations API

Multiple plugins use an operations API rather than direct method calls. The API
is designed to simplify two messy tasks: asynchronous operations and creating a
recoverable state. The operations API consists of a few classes in
`ca.on.oicr.gsi.vidarr`:

- `OperationAction`: the core class that describes an operations process
- `OperationStep`: a class that describes an asynchronous operation
- `OperationStatefulStep`: a class that describes an asynchronous operation
  which reads or modifies the on-going state

The process starts with the plugin generating an original state object. This is
a plugin-defined record. State objects should _not_ be mutated and using a
record helps to encourage this. The plugin defines an `OperationAction` that
describes the process, starting with the original state object, that outlines
the steps needed to transform that state into the final output expected by the
plugin.

As Víðarr runs the operations, it keeps track of two values: the state and the
_current value_. The current value is the output of the previous step and the
input to the next step. In effect, given steps _A_, _B_, and _C_, the
operations API will allow writing this as `load(...).then(A).then(B).then(C)`,
but it will be executing it as `C(B(A(...)))`, where the return value of the
previous step is the only parameter to the next one. This design is preferable
to direct calls because Víðarr can stop executing the task when it needs to
wait and restart it later, removing the burden of asynchronous scheduling from
the plugin author.

To start, a call to `OperationAction.load` or `OperationAction.value` is
required. This primes the sequence of events by computing a value from the
state information alone which will be the input for the first step. After this,
`then` may be called to manipulate this value. Additionally, there are
convenience methods `map`, to modify the current value, and `reload`, to discard
the current value and load a new one from the state.

`OperationStep` is technically a subset of `OperationStatefulStep`, but it is
implemented separately because the restricted design of `OperationStep` has
simpler type constraints, producing better errors during development.

How state is managed along this chain of events is intentionally hidden to
simplify the process. A `OperationStatefulStep` can wrap the state in
additional information. For instance, `repeatUntilSuccess` needs to track the
number of times it attempted an operation, so it wraps the state in
`RepeatCounter`. When the chain is executed, Víðarr takes the original state
and wraps it in classes like `RepeatCounter` to build up a state that tracks
all of the paths required by the chain. It can then write this wrapped state to
the database and, if Víðarr restarts, it can recover the operations
automatically using this state information.

This means that the steps along the way are automatically wrapping and
unwrapping state along the way so that the correct information is stored in the
database. One caveat is that if the structure of the operation of this code
changes, then so does the state stored in the database. Meaning that
redesigning the operations may mean that Víðarr cannot recover. Having the
plugin programmer manually manage state does allow them to have better control
over this scenario, but for a lot of overhead in the plugin implementation. The
interfaces Víðarr uses intentionally hide the state type behind a wild card
(`?`) generic to simplify writing plugins, but any changes to this type will
cause recovery issues.

# Provided Implementations

This core implementation provides several plugins independent of external
systems.

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
      "type": "ranked-max-in-flight"
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

```
{
  "type": "ranked-max-in-flight",
  "maxInFlight": 500
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
