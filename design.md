# Overall Design and Goals

Niassa/SeqWare started off as 3 things: a LIMS, a workflow engine, and a
metadata tracking system. We have replaced the LIMS with our
[Pinery](https://github.com/oicr-gsi/pinery) interface over
[MISO](https://github.com/miso-lims/miso-lims) and, previously, GeoSpiza. We
have outsourced the workflow engine with the Broad's
[Cromwell](https://cromwell.readthedocs.io/en/stable/). Víðarr is our
replacement for the metadata tracking.

It schedules workflows using a workflow engine and collects the
output from these workflows and stores the metadata about these files and
connections to the Pinery LIMS information. It supports using Cromwell
as a workflow engine, with the future goal to support other workflow engines.
It has to support provisioning out other kinds of data and associate LIMS
metadata, with the future goal to provision out logs, QC information, and
database fragments (_i.e._, ETL data). It supports communicating with multiple
workflow engines at once so it can schedule on a local HPC and a cloud
instance.

## Design Summary
The Víðarr analysis tracking system has:

* Incrementally fetching analysis records - this allows getting new analysis results available for downstream workflows more quickly.
* Support for multiple workflow execution engines - allowing new engines to be swapped in and laying the framework to extend compute capacity.
* A simplified workflow packaging and installation process - import workflows via a definition file.
* Updated software stack with minimal core dependencies and use of modules to limit dependency complexity - more easily update and maintain project while reducing breaking changes.

Components of Víðarr include:

- a [web service](vidarr-server/) to track analysis
- a [command line](vidarr-cli/) interface for testing and development
- a base workflow engine ([Cromwell](vidarr-cromwell/))

Additional components include:

- [Tools](https://github.com/oicr-gsi/vidarr-tools/) to generate workflow definitions
- [Cerberus](http://github.com/oicr-gsi/cerberus) create file provenance by integrating Víðarr output with Pinery

The goal for Víðarr web service is to:

- Take workflow requests.
- Check if this request is already completed.
- Delegate the workflow to a workflow engine.
- Provision the output and record the output in a database.
- Provide an interface to this database similar to provide a history of completed workflows.

## Shesmu Integration
Shesmu needs file provenance and can use Cerberus as a plugin. Shesmu also
needs to run workflows and can use the `/api/submit` endpoint for that purpose. It
needs to know what it can run and uses `/api/workflows` to get the known
workflow and `/api/targets` to know where it can run them; it generates an
action definition for every valid workflow version and target combination. The
submit action in Shesmu has commands to re-run failed workflows (_Reattempt_)
and unload-and-rerun (_Reprocess_). For more details, see the [Víðarr plugin
for
Shesmu](https://github.com/oicr-gsi/shesmu/blob/master/docs/plugin-vidarr.md).

## How a workflow runs
Víðarr takes a submission request that includes:

- the workflow name and version
- the target name
- the parameters to the workflows
- the external keys (LIMS keys) associated with the workflow
- the output metadata, that indicates what output is attached to which output
- any labels required by the workflow
- any parameters to the workflow engine
- consumable resource values

Once received:

1. The target and workflow name and version are checked. If not found or the workflow is not supported by the workflow engine, it is rejected.
2. The parameters, metadata, labels, consumable resource values, and engine parameters are type checked.
3. References to other Víðarr files used as input are resolved.
4. External keys are checked.
5. The [identifier](identifiers.md) is calculated.
6. The database is checked for the identifier.
   If it is found and in a failed state and the request asked for reattempting, the workflow is started.
   If it is found, the identifier is returned to the client.
   If the identifier is not found, the workflow run is started.

Once a workflow is started (or restarted), it goes through phases:

- the workflow waits for any consumable resource to allow it to continue
- the workflow does a preflight check where it ensures that configuration in the output metadata is valid
- it provisions in any input files
- it runs the workflow
- it provisions out the results of the workflow
- it performs any clean up required

We keep running into issue of running out of temp disk space. Shesmu can
throttle, but it's often too late. The Víðarr server has a number of consumable
resources (_e.g._, scratch disk) and then workflow runs can reserve a chunk of
that resource. The reservation decreases the available count of that resource
until the workflow is finished running. If there isn't enough of the resource,
it must wait until later. Consumable resources can also be used to block
starting workflows in ways similar to Shesmu throttlers. The implementation is
intentionally flexible.
