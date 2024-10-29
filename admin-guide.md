# Víðarr Administrator's Guide
Running a Víðarr instance involves a few separate roles:

- care and feeding of the Víðarr server (system operations)
- care and feeding of the workflow runs (workflow operations)
- creation and redesign of workflows (workflow development)

This guide is broken down by these roles, but they may overlap to some degree.

See [Víðarr Code Style](code-style.md) for preferred code formatting.

## System Operations Guide
Running a Víðarr instance requires:

- Java 17 or later
- PostgreSQL 12 or later
- Maven (for building, not required on installation machine)
- Docker (for building, not required on installation machine)

### Installing Víðarr on a Linux Instance
Build your own copy of Víðarr using Maven:

    mvn install
    mvn dependency:copy-dependencies

Pull all the JARs required for Víðarr and its plugins into a directory, say `/srv/vidarr/jars`:

    mkdir -p /srv/vidarr/jars
    cp vidarr-server/target/{,dependency}/*.jar /srv/vidarr/jars
    # If you intend to use the command line interface
    cp vidarr-cli/target/{,dependency}/*.jar /srv/vidarr/jars
    # And any plugins you want to use
    cp vidarr-cromwell/target/{,dependency}/*.jar /srv/vidarr/jars
    cp vidarr-sh/target/{,dependency}/*.jar /srv/vidarr/jars

It can be easiest to launch Víðarr with a shellscript as follows stored in `/srv/vidarr/run`:

    #!/bin/sh
    exec /usr/lib/jvm/java-17-openjdk-amd64/bin/java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:7000 --module-path $(ls /srv/vidarr/jars/*.jar|tr '\n' :) -m ca.on.oicr.gsi.vidarr.server/ca.on.oicr.gsi.vidarr.server.Main "$@"

To start Víðarr using systemd, create a `vidarr.service` as follows:

    [Unit]
    Description=Víðarr workflow provenance server
    
    [Service]
    User=debian
    ExecStart=/srv/vidarr/run /srv/vidarr/config
    KillMode=process
    Restart=always
    RestartSec=10
    
    [Install]
    WantedBy=multi-user.target

Create a new PostgreSQL database and role for Víðarr.

    CREATE ROLE vidarr LOGIN PASSWORD 'mash keyboard here';
    CREATE DATABASE vidarr OWNER vidarr;
    \c vidarr;
    ALTER SCHEMA public OWNER TO vidarr;
    GRANT ALL ON SCHEMA public TO vidarr;

Now prepare the JSON configuration file `/srv/vidarr/config` as follows:

    {
      "dbHost": "pg_db_host",
      "dbName": "pg_db_name",
      "dbPass": "pg_db_pass",
      "dbPort": 5432,
      "dbUser": "pg_db_user",
      "inputProvisioners": {},
      "name": "my_instance",
      "otherServers": {},
      "outputProvisioners": {},
      "port": 8088,
      "runtimeProvisioners": {},
      "targets": {},
      "url": "http://myinstance.vidarr.example.com/",
      "workflowEngines": {}
    }

Replace all the `pg_db_` values with the appropriate values to connect to the
database. 

`"port"` determines the port on which the Víðarr HTTP server will
run. Choose something appropriate, especially if using a reverse proxy. 

`"url"` should be set to the URL this Víðarr server is accessible on, after 
reverse proxying; Víðarr will use this to generate any self-referential URLs. 

Víðarr also operates in a federated fashion so `"name"` should be set to a 
unique identifier for this server independent of its URL. 

`"otherServers"` can be set up to connect to other Víðarr servers, using the
other server's `"name"` identifier as the key and the other server's URL as 
the value.

Now, the plugins must be configured. Here are example configurations for using
Cromwell in an HPC environment. Note that the names are arbitrary and multiple
plugins can be defined with different names:

      "inputProvisioners": {
        "raw": {
          "formats": [
            "FILE",
            "DIRECTORY"
          ],
          "type": "raw"
        }
     },
      "outputProvisioners": {
        "cromwell-output": {
          "cromwellUrl": "http://cromwell-output.example.com:8000",
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
      },
     "workflowEngines": {
        "local-cromwell": {
          "type": "cromwell",
          "url": "http://cromwell.example.com:8000"
        }
      }

Now, these different plugins can be linked together into a _target_. The target
names will be visible to clients.

       "targets": {
        "local_hpc": {
          "inputProvisioners": [
            "raw"
          ],
          "outputProvisioners": [
            "cromwell-output"
          ],
          "consumableResources": [],
          "runtimeProvisioners": [],
          "workflowEngine": "local-cromwell"
        }
      },
 
Once configured, Víðarr should be able to start. On an empty database, it will
automatically install its schema.

When new versions are released, simply replace the JARs and restart the
service. It will automatically upgrade the database schema on start.

## Creating a Development Environment
For workflow developers, it is also useful to install a `/srv/vidarr/cli` script:

    #!/bin/sh
    exec /usr/lib/jvm/java-17-openjdk-amd64/bin/java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:7071 --module-path $(ls /srv/vidarr/jars/*.jar|tr '\n' :) -m ca.on.oicr.gsi.vidarr.cli/ca.on.oicr.gsi.vidarr.cli.Main "$@"

This file does _not_ have to be on the Víðarr server and a copy of JARs and this script can be placed on another server. They will also need a configuration file to be able to run workflows in testing. It is a reduced form of the main configuration file:

    {
      "engine": {
        "type": "cromwell",
        "url": "http://cromwell-dev.example.com:8000",
        "debugInflightRuns": false,
        "engineParameters": {
          "read_from_cache": "boolean",
          "write_to_cache": "boolean"
        }   
      },
      "inputs": [
        {
          "formats": [
            "DIRECTORY",
            "FILE"
          ],
          "type": "raw"
        }
      ],
      "outputs": [
        {
          "cromwellUrl": "http://cromwell-dev.example.com:8000",
          "chunks": [2, 4],
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
      ],
      "runtimes": []
    }

For testing, only a single target can be configured, so the configuration for
each plugin is not put though a layer of indirection. Otherwise, the plugin
configuration are exactly as they would be for a normal server.

# Workflow Operations
TODO -- there is nothing useful at this point for workflow operations

# Workflow development
Workflow developers need to test workflows, install those workflows in Víðarr,
and make use of them via Shesmu.

## Building Workflows
TODO -- this is not implemented yet

## Running Workflows Manually
The command line interface allows manually running workflows. See
`/srv/vidarr/cli run --help` for details. Normally, the caller provides 3
pieces of information:

- the arguments to the workflow
- the external information to associate with the output of the workflow (metadata)
- extra parameters to provide to the workflow engine

These are provided as either JSON files or JSON arguments on the command line.
The exact format of these files will vary depending on the workflow and the
plugin configuration.

After the workflow runs successfully, the provenance records generated by the
workflow will be written out to a JSON file.

## Testing Workflows
The command line interface allows for testing workflows. See `/srv/vidarr/cli
test --help` for details. To run a series of automated tests, use test
definition file. Many tests can be defined in one file.

    [
      {
        "arguments": {},
        "description": "Basic test",
        "engineArguments": {},
        "id": "test1",
        "metadata": {},
        "validators": [
          {
            "metrics_calculate": "/srv/vidarr/test/calculate-md5",
            "metrics_compare": "/srv/vidarr/test/compare",
            "output_metrics": "/srv/vidarr/test/someworkflow/test1",
            "type": "script"
          }
        ]
      }
    ]

`"arguments"`, `"engineArguments"`, and `"metadata"` should be configured for
each test to be run. Since each test is run individually, internal Víðarr IDs
cannot be used for inputs as there will be no inputs. `"id"` is a unique
identifier for the test and will be provided to plugins for generating
provision out directories. `"description"` is a human-friendly name for the
test to appear in logs.

After each test runs, the output will be validated. Currently, the only
validator supported is `"script"`, which runs a calculate script on the output
files and then runs a comparison script that checks that the calculate script's
output matches the reference data.

Users have an option to provide a filepath to a directory where they would like
the provisioned out file. For example, adding the argument:
`-o /scratch2/groups/gsi/development/` 
will provision the output file to the gsi scratch2 development directory.  

If none is provided then the provisioned out file will be symlinked into a 
temporary directory. The `"metrics_calculate"` script will be run in that 
directory receiving the path
to that directory as the first argument. Anything it writes to standard output
will be captured. Then `"metrics_calculate"` will be called with the
`"output_metrics"` as the first argument and the file generated in the previous
step as the second argument. If it exits with a non-zero return code, then the
test is allowed to pass.
