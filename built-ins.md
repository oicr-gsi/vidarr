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

## Raw Input Provider
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
