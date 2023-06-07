# Anatomy of a Submission

The following is a real submission request for bcl2fastq and a dissection of
what is in the request.

```
{
  "arguments": {
    "bcl2fastq.basesMask": "y51,i8n*,i8n*,y51",
    "bcl2fastq.lanes": [
      2
    ],
    "bcl2fastq.mismatches": 1,
    "bcl2fastq.modules": "bcl2fastq/2.20.0.422 bcl2fastq-jail/3.1.2b barcodex-rs/0.1.2",
    "bcl2fastq.process.bcl2fastq": null,
    "bcl2fastq.process.bcl2fastqJail": null,
    "bcl2fastq.process.extraOptions": null,
    "bcl2fastq.process.ignoreMissingBcls": null,
    "bcl2fastq.process.ignoreMissingFilter": null,
    "bcl2fastq.process.ignoreMissingPositions": null,
    "bcl2fastq.process.memory": 14,
    "bcl2fastq.process.temporaryDirectory": "$TMP",
    "bcl2fastq.process.threads": null,
    "bcl2fastq.runDirectory": {
      "contents": {
        "configuration": "/.mounts/labs/prod/archive/A00469/200820_A00469_0115_BHTMWLDMXX",
        "externalIds": [
          {
            "id": "4703_2_LDI44042",
            "provider": "pinery-miso"
          }
        ]
      },
      "type": "EXTERNAL"
    },
    "bcl2fastq.samples": [
      {
        "acceptableUmiList": null,
        "barcodes": [
          "TGGCATGT-TGAAGACG"
        ],
        "inlineUmi": false,
        "name": "PBCM_0044_Ly_n_PE_3072005_CM_200820_A00469_0115_BHTMWLDMXX_2_TGGCATGT-TGAAGACG",
        "patterns": null
      }
    ],
    "bcl2fastq.timeout": 20
  },
  "attempt": 0,
  "consumableResources": {
      "priority": 1
    },
  "engineParameters": {
    "final_call_logs_dir": "/scratch2/groups/gsi/production/cromwell/cromwell-prod.hpc.oicr.on.ca_call_logs",
    "final_workflow_log_dir": "/scratch2/groups/gsi/production/cromwell/cromwell-prod.hpc.oicr.on.ca_workflow_logs"
  },
  "externalKeys": [
    {
      "id": "4703_2_LDI44042",
      "provider": "pinery-miso",
      "versions": {
        "pinery-hash-7": "cf4b332c847a9594463167453b993a285a11d7c13b29fcfe2fa951b2345ce17d",
        "shesmu-sha1": "15C99BE88D03CE10E575155DD3BCBF5BA7ED3D93"
      }
    }
  ],
  "labels": {},
  "metadata": {
    "bcl2fastq.fastqs": [
      {
        "fastqs": {
          "contents": [
            {
              "outputDirectory": "/oicr/data/archive/seqware/seqware_analysis_12/hsqwprod/"
            },
            [
              {
                "id": "4703_2_LDI44042",
                "provider": "pinery-miso"
              }
            ]
          ],
          "type": "MANUAL"
        },
        "name": "PBCM_0044_Ly_n_PE_3072005_CM_200820_A00469_0115_BHTMWLDMXX_2_TGGCATGT-TGAAGACG"
      }
    ]
  },
  "mode": "RUN",
  "target": "hpc",
  "workflow": "bcl2fastq",
  "workflowVersion": "3.1.3"
}
```

Starting at the end, `"mode"` property indicates whether we want to run the
workflow. `"DRY_RUN"` and `"VALIDATE"` are also possible. Both will check that
the workflow could be executed. `"DRY_RUN"` will also search the database to
determine if the workflow has been previously run. Neither will start it.

The `"workflow"` and `"workflowVersion"` properties sets which workflow to
execute. The workflow selected will set the `"labels"` required. `bcl2fastq`
doesn't require any labels, so this is an empty object. The labels must match
the labels when the workflow was registered. The version determines what inputs
and outputs the workflow requires and informs the `"arguments"` and
`"metadata"`. More details to follow.

The `"target"` determines the execution target on the Víðarr server. The
`"consumableResources"`, `"arguments"`, `"metadata"` and `"engineParameters"`
will have to match the target configuration.

The `"attempt"` field allows relaunching a failed workflow run. If this
workflow run matches an existing workflow run and that run has either failed or
not started (waiting on resources), then if `"attempt"` is one more than the
previous attempt, the previous attempt will be discarded and the workflow
retried with the arguments and metadata provided.

The workflow version defines what input it takes and what output it provides.
The exact data that must be provided is defined by a combination of the target
and the workflow version.

For most simple inputs, the data is exactly what is expected by the workflow
(_e.g._, if the workflow requests an integer, the submission must provide an
integer). The two special cases are files and directories. In this case, the
submitter must provide information to the input provisioner plugin on how to
fetch the file. The input can be a file output by another workflow in the
Víðarr database by specifying the Víðarr ID. In this case `"type":"external"`.
For `bcl2fastq`, the input is _not_ the output of an existing workflow, so
`"type":"EXTERNAL"` is found in `"arguments"."bcl2fastq.runDirectory"`. The
`"contents"."configuration"` is the information required by the input
provisioner. The `"configuration"."externalIds"` are the external IDs
associated.

Víðarr is built to associate data with an external LIMS system. The
`"externalKeys"` lists all the keys that are known to the workflow. A key is a
provider + identifier + versions. The versions are used in matching (details in
the [architecture guide](architecture.md)). All the input provider+identifiers
must be in this list. For `EXTERNAL` inputs, the provider+identifiers must be
listed with each input, just as in the `bcl2fastq` request above. For
`INTERNAL` inputs, Víðarr knows the provider+identifier from the database.

All the _output_ must also be associated with these external identifiers. Most
workflows use `"type": "ALL"` to associate the outputs with all the
provider+identifiers found in the input. Splitting workflows (root workflow
including `bcl2fastq` and bam-merge-preprocessing), use `"type" : "MANUAL"` to
assign the output appropriately. All keys must be accounted for in the output.
The output provisioner determines what additional data must be provided to
perform the output provisioning.
