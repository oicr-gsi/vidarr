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

