# Vidarr Plugins for Cromwell
These plugins allow interfacing Vidarr with [Cromwell](https://cromwell.readthedocs.io/en/stable/).

## Cromwell Output Provisioner
The output provisioner uses a Cromwell job to copy an output from a file off
shared disk to another directory for permanent archival. The workflow must
compute the file size and checksum of the file's contents and report them plus the
location where the file is now stored.

    {
      "cromwellUrl": "http://cromwell-output.example.com:8000",
      "chunks": [ 2, 4 ],
      "debugCalls": false,
      "fileField": "provisionFileOut.inputFilePath",
      "fileSizeField": "provisionFileOut.fileSizeBytes",
      "checksumField": "provisionFileOut.fileChecksum",
      "checksumTypeField": "provisionFileOut.fileChecksumType",
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
know the WDL version, provided as `"wdlVersion"`. 

The workflow will be called with two arguments, the file to copy (which will be provided as the argument
(`"fileField"`)) and the submitter-provided archive directory
(`"outputPrefixField"`). You may also provide two additional arguments if the workflow
does checksum verification: the expected checksum of the file to copy (`inputFileChecksum`) 
and the type of the expected checksum (`inputFileChecksumType`). See the Cromwell Output Provisioner 
config below if your workflow provides all four arguments.

    {
      "cromwellUrl": "http://cromwell-output.example.com:8000",
      "chunks": [ 2, 4 ],
      "debugCalls": false,
      "fileField": "reprovisionFileOut.inputFilePath",
      "inputChecksumField": "reprovisionFileOut.inputFileChecksum",
      "inputChecksumTypeField": "reprovisionFileOut.inputFileChecksumType",
      "fileSizeField": "reprovisionFileOut.fileSizeBytes",
      "checksumField": "reprovisionFileOut.fileChecksum",
      "checksumTypeField": "reprovisionFileOut.fileChecksumType",
      "outputPrefixField": "reprovisionFileOut.outputDirectory",
      "storagePathField": "reprovisionFileOut.fileOutputPath",
      "type": "cromwell",
      "wdlVersion": "1.0",
      "workflowOptions": {
        "read_from_cache": false,
        "write_to_cache": false
      },
      "workflowUrl": "http://example.com/reprovisionFileOut.wdl"
    }

Once the workflow has completed, the plugin will
collect the permanent location for the file (`"storagePathField"`), the checksum
(`"checksumField"`), the checksum algorithm (`"checksumTypeField"`) 
and the file size (`"fileSizeField"`).

After running the specified WDL, the Cromwell OutputProvisioner needs to fetch 
metadata from the Cromwell server. Including the `calls` block of the metadata
in the response can have negative performance implications for sufficiently 
large workflow runs, so by default, `calls` is only fetched if provisioning out
has failed. Set `"debugCalls"` to true in order to retrieve `calls` information
for running provision out tasks as well.

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
        String fileChecksum = rsync_file.fileMd5sum
        String fileChecksumType = "md5sum"
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
        String fileChecksum = read_string("md5sum.out")
        String fileChecksumType = "md5sum"
        String fileOutputPath = read_string("filePath.out")
      }
    
      runtime {
        memory: "1 GB"
        timeout: "1"
      }
    }

Here is an example WDL script to do provisioning out that verifies the input checksum of the file and uses hard linking:

    version 1.0
    workflow reprovisionFileOut {
      input {
        String inputFilePath
        String outputDirectory
        String inputFileChecksumType
        String inputFileChecksum
      }
      
      call verify_checksum {
        input:
          inputFilePath=inputFilePath,
          inputFileChecksumType=inputFileChecksumType,
          inputFileChecksum=inputFileChecksum
      }
    
      call hardlink_file {
        input:
          inputFilePath=verify_checksum.filePath,
          outputDirectory=outputDirectory
      }
    
      output {
        String fileChecksum = inputFileChecksum
        String fileChecksumType = inputFileChecksumType
        String fileSizeBytes = hardlink_file.fileSizeBytes
        String fileOutputPath = hardlink_file.fileOutputPath
      }
    }
    
    task verify_checksum {
      input {
        String inputFilePath
        String inputFileChecksumType
        String inputFileChecksum
      }
    
      command <<<
      set -euo pipefail
    
      CRC32_CHECKSUM_TYPE="crc32"
      INPUT_FILE="~{inputFilePath}"
      FILE_CHECKSUM_TYPE="~{inputFileChecksumType}"
      EXPECTED_FILE_CHECKSUM="~{inputFileChecksum}"
    
      if [ "${FILE_CHECKSUM_TYPE}" == "${CRC32_CHECKSUM_TYPE}" ]; then
        # The crc32 tool may contain a "GOOD" or "BAD" string if it detects a hexidecimal in the input file path
        # because it interprets the hexidecimal as an expected checksum to perform a comparison against.
        # https://unix.stackexchange.com/questions/481141/why-does-crc32-say-some-of-my-files-are-bad
        # Thus, take the first whitespace-delimited field, which is the calculated checksum
        CALCULATED_FILE_CHECKSUM=$(crc32 "${INPUT_FILE}" | awk '{print $1}')
      else
        echo "File checksum type ${FILE_CHECKSUM_TYPE} is not supported" >&2
        exit 1
      fi
    
      if [ "${CALCULATED_FILE_CHECKSUM}" != "${EXPECTED_FILE_CHECKSUM}" ]; then
        echo "The calculated ${FILE_CHECKSUM_TYPE} file checksum ${CALCULATED_FILE_CHECKSUM} does not match the expected file checksum ${EXPECTED_FILE_CHECKSUM}" >&2
        exit 1
      fi
    
      echo "Verified that the calculated file checksum ${CALCULATED_FILE_CHECKSUM} matches the expected file checksum ${EXPECTED_FILE_CHECKSUM}"
      >>>
    
      output {
        String filePath = "~{inputFilePath}"
      }
    
      runtime {
        memory: if ceil(size(inputFilePath)/(2.0*1024*1024*1024)) < 4 then "4 GB"
                else if ceil(size(inputFilePath)/(2.0*1024*1024*1024)) > 64 then "64 GB"
                else ceil(size(inputFilePath)/(2.0*1024*1024*1024)) + " GB"
        timeout: ceil(size(inputFilePath)/(2.5*1024*1024)/60/60)+4
        io_slots: if size(inputFilePath) >= (64.0*1024*1024*1024) then 6
                  else floor((6.0 - 1.0) * size(inputFilePath)/(64.0*1024*1024*1024) + 1.0)
      }
    }
    
    task hardlink_file {
      input {
        String inputFilePath
        String outputDirectory
      }
    
      command <<<
      set -euo pipefail
    
      INPUT_FILE="~{inputFilePath}"
      OUTPUT_DIRECTORY="~{outputDirectory}"
      OUTPUT_FILE_PATH="${OUTPUT_DIRECTORY%%/}/$(basename ${INPUT_FILE})"
      GSI_USER=$(whoami)
    
      test -d "${OUTPUT_DIRECTORY}" || mkdir -p "${OUTPUT_DIRECTORY}"
    
      if [ ! -f "${INPUT_FILE}" ]; then
        echo "${INPUT_FILE} not a file or not accessible" >&2
        exit 1
      fi
    
      if [ -f "${OUTPUT_FILE_PATH}" ]; then
        echo "${OUTPUT_FILE_PATH} already exists" >&2
        exit 1
      fi
    
      echo "Changing ownership of ${INPUT_FILE} to ${GSI_USER}:gsi"
      if ! sudo -n /usr/bin/chown "${GSI_USER}":gsi "${INPUT_FILE}"; then
        echo "Insufficient permissions to change ownership of file ${INPUT_FILE}" >&2
        exit 1
      fi
    
      ln "${INPUT_FILE}" "${OUTPUT_FILE_PATH}"
      echo "Created a hard link from ${INPUT_FILE} to ${OUTPUT_FILE_PATH}"
    
      stat --printf="%s" "${OUTPUT_FILE_PATH}" > size.out
      echo "${OUTPUT_FILE_PATH}" > filePath.out
      >>>
    
      output {
        String fileSizeBytes = read_string("size.out")
        String fileOutputPath = read_string("filePath.out")
      }
    
      runtime {
        backend: "Local"
      }
    }

## Cromwell Workflow Engine
This can be used to run WDL workflows using a remote Cromwell instance. The configuration is as follows:

    {
      "debugInflightRuns": false,
      "engineParameters": {
         "parameter1": type...
      },
      "type": "cromwell",
      "url": "http://cromwell.example.com:8000"
    }

`"url"` specified the Cromwell server that should be contacted.
`"engineParameters"` is optional and allows extra parameters to be required.
These will be passed as Cromwell's `workflowOptions`.
As the Cromwell WorkflowEngine accesses Cromwell to assess progress on workflow
runs, it fetches `/metadata` from Cromwell. For sufficiently large workflow runs,
fetching this endpoint with `calls` information included has negative performance
implications, so by default we only fetch `calls` for failed workflow runs to use
as debugging information. To fetch `calls` information for running workflow runs,
set `"debugInflightRuns"` to true.
