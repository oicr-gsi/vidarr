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
provide multiple. Plugins are loaded from JSON data in the Vidarr configuration
file or, in the case of an unload filter, user requests, using Jackson. Each
plugin can load whatever Jackson-compatible data from JSON it requires. Each
plugin has a small "provider" class which provides type information for
Jackson. In the JSON file, the `"type"` attribute will be used to create the
appropriate class instance. The provider class lists what values for `"type"`
correspond to what Java objects that Jackson should load. Since objects are
instantiated by Jackson, most have a `startup` method that is called after
loading is complete where the plugin can do any initialisation required. If it
throws exceptions, the Vidarr server will fail to start, which is probably the
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

Most plugins have a `recover` method. If Vidarr is restarted, the plugin will
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

Consumable resources are long-running. Whenever Vidarr attempts to run a
workflow, it will consult the consumable resources to see if there is capacity
to run the workflow (the `request` method). At that point the consumable
resource must make a decision as to whether the workflow can proceed. Once the
workflow has finished running (successfully or not), Vidarr will `release` the
resource so that it can be used again. When Vidarr restarts, any running
workflows will be called with `recover` to indicate that the resource is being
used and the resource cannot stop the workflow even if the resource is
over-capacity. 

Consumable resources can request data from the user, if desired. The
`inputFromSubmitter` can return an empty optional to indicate that no
information is required or can indicate the name and type of information that
is required. The `request` and `release` methods will contain a copy of this information,
encoded as JSON, if the submitter provided it. The JSON data has been
type-checked by Vidarr, so it should be safe to convert to the expected type
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

The class `BaseJsonInputProvisioner` is a partial implementation that can store
crash recovery information in a JSON object of the implementor's choosing,
making recovery easier.

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

The class `BaseJsonOutputProvisioner` is a partial implementation that can
store crash recovery information in a JSON object of the implementer's
choosing, making recovery easier.

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

The class `BaseJsonRuntimeProvisioner` is a partial implementation that can
store crash recovery information in a JSON object of the implementer's
choosing, making recovery easier.

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
output of the workflow. Vidarr will identified the output files generated by
the workflow engine and they will be passed to the output provisioners.

After the output provisioners have completed, the workflow engine will be
called again to cleanup any output, if this is appropriate. If the workflow
engine does not support cleanup, it should gracefully succeed during the
clean-up (and clean-up recovery) methods.

The class `BaseJsonWorkflowEngine` is a partial implementation that can store
crash recovery information in a JSON object of the implementer's choosing,
making recovery easier.

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
`/consumable-resources/`_name_`/allowed/`_run_ where _name_ is the consumable
resource name and _run_ is the workflow run ID. The current list can be
retrieved by making a `GET` request to
`/consumable-resources/`_name_`/allowed`.

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

