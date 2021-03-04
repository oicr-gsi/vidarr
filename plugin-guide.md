# Vidarr Plugin Developer's Guide
Vidarr uses plugins to allow interaction with a diverse set of systems. Plugins
are loaded by the Java
[`ServiceLoader`](https://docs.oracle.com/javase/9/docs/api/java/util/ServiceLoader.html)
and exported by the [Java module
system](https://www.oracle.com/corporate/features/understanding-java-9-modules.html)
using the `provides` keyword. All plugins need to depend only on the
`ca.on.oicr.gsi.vidarr.pluginapi` module to hook into the Vidarr
infrastructure.

There are several services that a plugin can provide and a plugin is free to
provide multiple. Each has two classes: a small "loader" class responsible for
loading this plugin's configuration from JSON files and a "real" interface that
does most of the work. For loading, each plugin must provide a unique name that
will be used in the `"type"` property of JSON configuration files.

These are high-level overviews of the purpose and general constraints for each
service. The JavaDoc for each interface provides the details for how the
interfaces should behave.

Additionally, plugins communicate with the outside world through the types they
expect. A description of the types is provided in
`ca.on.oicr.gsi.vidarr.SimpleType` and the format for the values is meant to be
compatible with Shemu's.

Plugins are meant to run asynchronously. Most plugins are given a `WorkMonitor`
instance which allows a plugin to communicate back to Vidarr and schedule
future asynchronous tasks. Plugins must implement recovery from crash, so are
expected to journal their current state to the database. The `WorkMonitor`
provides methods to journal state to the database for crash recovery and to
provide status information to users.

See [Vidarr Code Style](code-style.md) for preferred code formatting.

# Consumable Resource
Consumable resources implement
`ca.on.oicr.gsi.vidarr.ConsumableResourceProvider` and
`ca.on.oicr.gsi.vidarr.ConsumableResource`. These plugins are responsible for
delaying workflow run execution until resources are available.

The plugins can be associated with targets in the server configuration.
Consumable resources _may_ request that submitters provide information or
operate on the existence of a workflow run.

# Input Provisioners
Input provisioners implement `ca.on.oicr.gsi.vidarr.InputProvisionerProvider`
and `ca.on.oicr.gsi.vidarr.InputProvisioner`. These plugins are responsible for
taking files from existing workflows or provided by the user and generating a
file path that a workflow can use.

This plugin and the workflow plugins must have a mutual understanding of what a
file path means. That is somewhat the responsibility of the system
administrator. For instance, if in an HPC environment with shared disk, the
system administrator must direct the provision in plugin to write to a shared
directory instead of, say, `/tmp` and ensure the right permissions are set up.
These are not the responsibility of the plugin author.

The class `BaseJsonInputProvisioner` is a partial implementation that can store
crash recovery information in a JSON object of the implementor's choosing,
making recovery easier.

# Output Provisioners
Output provisioners implement `ca.on.oicr.gsi.vidarr.OutputProvisionerProvider`
and `ca.on.oicr.gsi.vidarr.OutputProvisioner`. These plugins are responsible
for taking data (files or JSON values) from completed workflows, moving the
data into permanent storage and writing back a file path or URL that will be
associated with the correct external identifiers.

The class `BaseJsonOutputProvisioner` is a partial implementation that can
store crash recovery information in a JSON object of the implementor's
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
store crash recovery information in a JSON object of the implementor's
choosing,
making recovery easier.

# Workflow Engine
Workflow engines implement `ca.on.oicr.gsi.vidarr.WorkflowEngineProvider`
and `ca.on.oicr.gsi.vidarr.WorkflowEngine`. These plugins are responsible for
running workflows and collecting the output from the workflow.

The class `BaseJsonWorkflowEngine` is a partial implementation that can store
crash recovery information in a JSON object of the implementor's choosing,
making recovery easier.

# Unloaders
Unloader implement `ca.on.oicr.gsi.vidarr.UnloaderProvider`
and `ca.on.oicr.gsi.vidarr.Unloader`. This feature is not complete, so these
interfaces are not ready for use.

TODO
