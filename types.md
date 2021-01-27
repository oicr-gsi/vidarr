# Understanding the Vidarr Type System
Vidarr has three separate type systems:

- basic types
- input types
- output types

Input types are used for arguments to workflows. Output types are used for
metadata from workflows. Basic types are used for engine parameters and as
part of input and output types when they are required by plugins.

The reasoning is something like this: input values can be anything a workflow
can take in. That's a primitive types (numbers, strings, Booleans), complex
types (lists, structures, pairs), and files (and directories). Files need to go
through a provisioning process and the plugin requires additional information
to provision in a file. The plugin can accept primitive types and complex
types, but _not_ additional files. So, basic types are primitives and
collections of primitives. Input types are primitives, files/directories, and
collections of those.

The outputs are...weird. The workflow is outputting some kind of data and the
submitter of the workflow needs to provide the _metadata_ to be associated with
that data. So the types of the output have very different constraints from the
input types and the output type information is interpreted differently in the
workflow's output vs the submission request. Like input types, the plugins
handling output types require information, so basic types are embedded in the
output type information.

## Basic Types
Basic types can be basic primitive types, in the following table, or complex
types described in detail after the table. All types have JSON encoding.

| JSON Type Encoding | Description                                           |
|--------------------|-------------------------------------------------------|
| `"boolean"`        | Boolean types, in standard JSON encoding              |
| `"date"`           | A string containing an ISO-8601 time including a zone |
| `"floating"`       | A floating point value; precision is not specified    |
| `"integer"`        | An integer value; precision is not specified          |
| `"json"`           | Arbitrary JSON data that is not type checked          |
| `"string"`         | A string                                              |

- `{"is": "dictionary", "key":` _key-type_`, "value": ` _value-type_ `}`
A dictionary/map type. This value has two possible encodings. If the _key-type_
is a string, it _may_ be encoded as a JSON object. It can also be encoded as a
list of lists, where the inner lists are all two elements: the key and value.

- `{"is": "list", "inner":` _type_ `}`
A list/array of items, encoded as a JSON array.

- `{"is": "object", "fields": {` _field-name1_ `:` _field-type1_`,` ... `} }`
A structure encoded as a JSON object. All fields must be present. The field
names must follow JSON property name rules.

- `{"is": "optional", "inner":` _type_ `}`
A value which may be absent. If absent, it is encoded as `null`. Nesting this
type has no meaningful effect; that is optional of optional of string is
functionally identical to optional of string.

- `{"is": "pair", "left":` _left-type_`, "right":` _right-type_ `}`
A pair of values. This exists mostly as a special case of WDL. It is encoded as
a JSON object with two properties `"left"` and `"right"`.

- `{"is": "tagged-union", "options": { `_name1_ `:` _type1_`,` ... `} }`
Create a tagged union type (or algebraic data type). The value provides one
particular value based on the options available. These are encoded as `{"type":
`_name_`, "contents": `_value_`}` where _name_ is the name of the selected
option and _value_ is a value of the matching type.

For details, see Shesmu's algebraic data types. Shesmu has the restriction that
the types must be tuples or objects, but Vidarr does not enforce this
restriction.

- `{"is": "tuple", "elements": [` _type1_`,` _type2_`,` ... `] }`
A heterogenous tuple of values. A tuple is effectively an object but with
numerical indices instead of field names. It is encoded as a JSON array.

## Input Types
All of the conventions for basic types applies to input types. Additionally,
`"file"` and `"directory"` types are supported.

In both cases, the exact type will depend on the plugins on the server. The
submitter can provide either:

- a file known to Vidarr by its ID: `{"type": "INTERNAL", "contents": ["`_id_`"]}`
- an external file and the external identifiers for this file: `{"type": "EXTERNAL", "contents": {"externalIds": `_externals_`, "configuration": `_config_`} }`

_id_ is a single Vidarr ID in the form `vidarr:`_instance_`/`_hash_.
_externals_ is a list of external IDs, encoded as an array of `{"id":
"`_identifier_`", "provider": "`_provider_`"}` objects, where _identifier_ and
_provider_ are arbitrary strings that match up to an external data source, such
as LIMS. _configuration_ is the value expected by the plugin. The workflow does
not know the correct type for _configuration_; the target has this information.

## Output Types
The output types are very different from the input and basic types. There are
a few base output types, in the table below. These base types affect what the
workflow outputs to Vidarr, but the submitter always provides metadata in this
form:

- `{"type": "ALL", "contents": [`_configuration_`]}`
This associates the output with all of the external IDs provided with the workflow run.
- `{"type": "REMAINING", "contents": [`_configuration_`]}`
This associates the output with the external IDs provided with the workflow run
not included in a `MANUAL` reference.
- `{"type": "MANUAL", "contents": [`_configuration_`, `_externals_`]}`
This associates the output with a specific set of the external IDs in
_externals_, which is a list of `{"id": "`_identifier_`", "provider":
"`_provider_`"}` objects.

In each case, the type of _configuration_ depends on the output type and the
target used. The workflow's output is described in the table.

| Output Type           |  Workflow Output                                                    |
|-----------------------|---------------------------------------------------------------------|
| `"file"`              | A single file                                                       |
| `"files"`             | A list of files                                                     |
| `"file-with-labels"`  | A pair of a file and a dictionary of strings                        |
| `"files-with-labels"` | A pair of a list of files and a dictionary of strings               |
| `"logs"`              | A single file containing text logs                                  |
| `"quality-control"`   | A Boolean value which indicates pass/fail                           |
| `"warehouse-records"` | A single file containing structured data to be stored in a database |

For the data warehouse, the plugin and workflow must be mutually aware of the
data format.

In the types with labels, the workflow can add arbitrary information that gets
attached to the provisioned out file. This is useful to provide information
about the contents of a file that can be used by downstream processing. For
instance, workflow producing a FASTQ or BAM file could include a sequence
count, so a Shesmu olive consuming this file could pick an appropriate shard
count for a subsequent workflow.

In some workflows that expand or multiplex content (_e.g_, co-cleaning,
BCL2FASTQ), the output metadata needs to be dynamically assigned to the output
files. In that case, the list type is available:

- `{"is": "list", "keys": {`_key-name1_`:` _key-type1_`,` ... `}, "outputs": {` _output-name1_`:` _output-type1_`, ` ... `}}`

The allows the submitter to supply a list of structures with metadata and the
workflow to supply a list of structures with output and Vidarr will marry them
based on the keys.

The _key-typeN_ must be either `"INTEGER"` or `"STRING"` and the _output-typeN_
are the output types above. There must be no overlap between _key-nameN_ and
_output-name_.

This is easier with a concrete example. Suppose this is a BCL2FASTQ workflow
producing multiple FASTQs based on the sample name. The type definition would
be as follows:

    {
      "is": "list",
      "keys": {
        "sample_name": "STRING"
      },
      "outputs": {
        "fastqs": "files"
      }
    }

Now, the submitter would produce a structure like this:

    [
      {
        "fastqs": {
          "contents": [
            ...,
            [
              {
                "id": "RUN0001_SAM0001",
                "provider": "lims"
              }
            ]
          ],
          "type": "MANUAL"
        },
        "sample_name": "SAM0001"
      },
      {
        "fastqs": {
          "contents": [
            ...,
            [
              {
                "id": "RUN0001_SAM0002",
                "provider": "lims"
              }
            ]
          ],
          "type": "MANUAL"
        },
        "sample_name": "SAM0002"
      }
    ]

The plugin-specific information has been elided for brevity.

The workflow would produce a structure like this:

    [
      {
        "fastqs": [
          "/srv/output/abcdefg/SAM0001_R1.fastq.gz",
          "/srv/output/abcdefg/SAM0001_R2.fastq.gz"
        ],
        "sample_name": "SAM0001"
      },
      {
        "fastqs": [
          "/srv/output/abcdefg/SAM0002_R1.fastq.gz",
          "/srv/output/abcdefg/SAM0002_R2.fastq.gz"
        ],
        "sample_name": "SAM0002"
      }
    ]

The workflow can recycle input associations (_i.e._, if there are two output
structures with `"sample_name": "SAM0001"`, that is fine). The workflow must
use all of the associations provided (_i.e._, if the submitter provided
`SAM0001` and `SAM0002` and the workflow only produces output for `SAM0001`,
this is an error).

If multiple keys are used, they are treated like a composite key.
