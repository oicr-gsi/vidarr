# Vidarr Plugins for Cromwell
These plugins allow interfacing Vidarr with [Cromwell](https://cromwell.readthedocs.io/en/stable/).

## Cromwell Output Provisioner
The output provisioner uses a Cromwell job to copy an output from a file off
shared disk to another directory for permanent archival. The workflow must
compute the file size and MD5 of the file's contents and report them plus the
location where the file is now stored.

    {
      "cromwellUrl": "http://cromwell-output.example.com:8000",
      "chunks": [ 2, 4 ],
      "fileField": "provisionFileOut.inputFilePath",
      "fileSizeField": "provisionFileOut.fileSizeBytes",
      "md5Field": "provisionFileOut.fileMd5sum",
      "outputPrefixField": "provisionFileOut.outputDirectory",
      "storagePathField": "provisionFileOut.fileOutputPath",
      "type": "cromwell",
      "wdlVersion": "1.0",
      "workflowOptions": {
        "read_from_cache": false,
        "write_to_cache": false
      },
      "workflowUrl": "http://example.com/provisionFileOut.wdl"
    }

The `"cromwellUrl"` is the Cromwell server that will handle these requests. The
workflow can be given by URL through `"workflowUrl"` or inline as a string
using `"workflowSource"`. `"workflowOptions"` is the workflow options Cromwell
requires. Although Cromwell is given the entire workflow, it still needs to
know the WDL version, provided as `"wdlVersion"`. The workflow will be called
with two arguments, the file to copy (which will be provided as the argument
(`"fileField"`) and the submitter-provided archive directory
(`"outputPrefixField"`). Once the workflow has completed, the plugin will
collect the permanent location for the file (`"storagePathField"`), the MD5
(`"md5Field"`), and the file size (`"fileSizeField"`).

Your file system probably will not appreciate having thousands of files dumped
in a single output directory, so the `"chunks"` parameter will create a
hierarchy of directories based on the workflow run identifier. The numbers
determine the number of characters to use in each directory. For example, `[2,
4]` will take an ID of the form `AABBBBCCCCCCCCCCCCCC` and produce an output
path that is `AA/BBBB/AABBBBCCCCCCCCCCCCCC`. Once files have been provisioned
out, it is possible to change the chunking scheme, but the existing files are
already recorded in the Vidarr database and should not be moved without
updating the database.

Here is an example WDL script to do provisioning out that uses rsync to do the file copying:

    version 1.0
    workflow provisionFileOut {
      input {
        String inputFilePath
        String outputDirectory
      }
   
      call rsync_file {
        input:
          inputFilePath=inputFilePath,
          outputDirectory=outputDirectory
      }
      output {
        String fileSizeBytes = rsync_file.fileSizeBytes
        String fileMd5sum = rsync_file.fileMd5sum
        String fileOutputPath = rsync_file.fileOutputPath
      }
    }
    
    task rsync_file {
      input {
        String inputFilePath
        String outputDirectory
      }
    
      command <<<
        set -euo pipefail
    
        INPUT_FILE="~{inputFilePath}"
        OUTPUT_DIRECTORY="~{outputDirectory}"
        OUTPUT_FILE_PATH="${OUTPUT_DIRECTORY%%/}/$(basename ${INPUT_FILE})"
    
        test -d "${OUTPUT_DIRECTORY}" || mkdir -p "${OUTPUT_DIRECTORY}"
    
        if [ ! -f "${INPUT_FILE}" ]; then
          echo "${INPUT_FILE} is not a file or not accessible"
          exit 1
        fi
    
        if [ -f "${OUTPUT_FILE_PATH}" ]; then
          echo "${OUTPUT_FILE_PATH} already exists"
          exit 1
        fi
    
        echo "Starting rsync"
        rsync -aL --checksum --out-format="%C" "${INPUT_FILE}" "${OUTPUT_FILE_PATH}" > md5sum.out
        echo "Completed rsync"
    
        stat --printf="%s" "${OUTPUT_FILE_PATH}" > size.out
    
        echo "${OUTPUT_FILE_PATH}" > filePath.out
      >>>
    
      output {
        String fileSizeBytes = read_string("size.out")
        String fileMd5sum = read_string("md5sum.out")
        String fileOutputPath = read_string("filePath.out")
      }
    
      runtime {
        memory: "1 GB"
        timeout: "1"
      }
    }

## Cromwell Workflow Engine
This can be used to run WDL workflows using a remote Cromwell instance. The configuration is as follows:

    {
      "engineParameters": {
         "parameter1": type...
      },
      "type": "cromwell",
      "url": "http://cromwell.example.com:8000"
    }

`"url"` specified the Cromwell server that should be contacted.
`"engineParameters"` is optional and allows extra parameters to be required.
These will be passed as Cromwell's `workflowOptions`.
