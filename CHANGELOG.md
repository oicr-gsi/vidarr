# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0).
For unreleased changes, see [changes](changes).

-----------------------------------------------------------------------------

## [0.11.0] - 2023-01-26

### Added

* Provide a loadable file with sample provenance data

### Changed

* Removed `/waiting` endpoint, as it isn't useful to our users, and errors in dev and production
* Remove Niassa plugin

### Upgrade Notes

* Update code to use Java 17 features


## [0.10.0] - 2022-11-03

### Added

* Add workflow ID to GET `/api/workflow/{name}/{version}` API endpoint. This can be used for deleting workflow runs by `vidarr-workflow-id`.
* Adds recovery-failures API endpoint which returns workflow run ids of any runs which failed to recover from the database on restart.

### Changed

* Wraps a significant chunk of DatabaseBackedProcessor.recover() in try-catch Exception block. This changes the error handling of SQLException on startup for now. Intend to fix this in a later release.

### Fixed

* Fixes Cromwell provision-out retry behaviour to account for situation where cromwell does not return an ID.


## [0.9.1] - 2022-09-14

### Fixed

* Fix bug where Vidarr workflow runs were not recorded as FAILED if the Cromwell workflow run failed and debugging workflows was turned on


## [0.9.0] - 2022-09-08

### Added

* Adds debug calls to cromwell workflow engine and output provisioner config
* * Adds a new `retry` type that allows a workflows that fail to be retried with different arguments

### Fixed

* ### Fixed
* Fixes Provision In phase not logging recovery information in a useable format.
* Fixes uncaught exceptions interrupting db-backed recovery.


## [0.8.1] - 2022-08-03

### Fixed

* Bugfix for null cromwell ids getting stuck in check() loop
* Readability of AutoInhibit alert messages
* Exclude calls from metadata unless we know the workflow run is failed for performance reasons
* Update integration tests:
  * Move testdata to new resource path to prevent classpath collision
  * Update database integration tests to use same postgresql version as other ITs and Vidarr deploys
* Use explicit versions for Jackson dependencies instead of the version range that dependabot suggested


## [0.8.0] - 2022-06-21

### Added

* Add index on Niassa's workflowRunSWID to `workflow_run` table
* Add an index on `analysis(analysis_type)` and `analysis(hash_id)` to improve the performance of matching submitted requests to existing workflow runs

### Changed

* Changed the EXTENSION_TO_METADATA mapping in CromwellOutputProvisioner to include .crai and .cram extensions.
* Bump jooq from 3.14.0 to 3.16.5
  Bump rest-assured packages from 4.4.0 to 5.0.0
  Bump jackson dependencies

### Fixed



## [0.7.0] - 2022-04-04

### Added

* Add configuration for Dependabot

### Changed

* Do case-insensitive matching for Prometheus Alertmanager alerts

### Fixed

* Fix issue where unloading a file and loading in a new file with the same hash_id would cause the old file path to be used.


## [0.6.0] - 2022-03-17

### Added

* Index on niassa-file-accession

### Changed

* Clarify design and configuration for prometheus plugin to throttle on AutoInhibit alerts

### Fixed

* Deduplicate migration file paths


## [0.5.0] 2022-01-28

### Added
* Return IDs of deleted workflow runs in unload endpoint (#134)

### Changed
* Update getWorkflowVersion output so that it can be posted to addWorkflowVersion (#137)
* Clarify variable names in computeWorkflowRunHashId (#140)
* Add to inflight count during recovery (#139)

### Fixed
* AddWorkflowVersion for an existing workflow + version should succeed if the request is identical to the existing workflow verison, and fail otherwise (#142)
* Fix recursive unload, and fix hash calculation in load (#141)
* Max-In-Flight bugfixes (#138)

## [0.4.12] 2022-01-07

### Changed
* [GP-3106] Switch workflow run and active operation ids to bigint (#136)

### Fixed
* Return a match earlier in the submission process when in no launch mode (dry run)

## [0.4.11] 2021-12-08

### Fixed
* Release semaphore/lock if sftp fails and perform all sftp work in synchronously
* Give more informative error message when input ID resolution fails (#132)

## [0.4.10] 2021-11-09

### Fixed
* Insert the full workflow run ID as a directory in the symlink path, and use that symlinked path as the canonical path for the analysis record (#131)

## [0.4.9] 2021-11-04

### Changed
* Upgrade git code formatter plugin to work with Java 16 (#129)
* Count number of errors when returning provenance records
* Change name to workflowName for consistency
* Bind more params in InsertInto statements

### Fixed
* GP-2851 fix external IDs comparison for MANUAL external keys (#130)
* GP-2854 find the correct output provisioner for a given workflow output data type
* Fix imports

## [0.4.8] 2021-10-07

### Changed
* Wrap repeated response generation code in functions
* Clarify how to configure otherServers

### Fixed
* Fix submission error ZonedDateTime not supported by default (#126)
* GP-2799 fix error merging pinery keys; test DatabaseBackedProcessor (#121)
* Bind user-provided parameters in Main.java to prevent SQL injection attacks
* [GP-2790] Switch to discovering external ids using ExtractInputExternalIds (#123)

## [0.4.7] 2021-09-10

### Added
* Integration tests with very bad data

### Fixed
* [GP-2790] Internal ID processing fixes (#119)

## [0.4.6] 2021-07-28

### Added
* Add GET workflow version endpoint (#114)
* Add antatomy of a submission
* [GP-2720] BasicType Unit Tests (#115)
* [GP-2720] OutputType unit tests (#113)
* More integation tests (#111)
* [GP-2720] Unit Tests for InputType (#97)
* Add some API integration tests (#99)

### Changed
* Update database schema permissions on create (#117)
* [GP-2743] Reduce magic strings (#116)
* Move dependency-plugin config outside <executions> (#112)
* Improve performance of `/api/status` endpoint
* Improve PAM input label (#108)

### Fixed
* Fix provenance endpoint for latest versions
* Only update workflow if values are different (#110)
* Fix URL in documentation

## [0.4.5] 2021-06-04

### Added
* Semaphores for mkdirs in NiassaOutputProvisioner (#105)

### Changed
* Remove version export restriction

## [0.4.4] 2021-06-03

### Changed
* Change version query generation to avoid SQL error
* Make attribute names embedded in the query
* Add index to `analysis_external_id` table

## [0.4.3] 2021-06-02

### Added
* Create directory entries in Cromwell ZIP
* Expand details for writing plugins
* Add notes about zero-length types from discussion

### Changed
* Allow really large unload queries
* Pass through stderr from calculate script

## [0.4.2] 2021-05-27

### Added
* Allow comparison script to stdout/stderr
* Provide more details for submission errors

### Fixed
* Ensure workflow exists before adding wf definition (#90)

## [0.4.0] 2021-05-25

### Added
* Create an endpoint to access a single workflow
* Include more Cromwell failure information
* Add Víðarr design document and fix spelling in documentation
* Add support for optional outputs
* Add URL parameters to OpenAPI documentation (#83)
* Add Prometheus Alertmanager consumable resource (#79)

### Changed
* Bump junit from 4.11 to 4.13.1
* Print version at end of release script

### Fixed
* Remove testcontainers dependency as it's not playing well with modules (#89)
* Prevent load/unload requests from stacking up
* Fix read locking bug
* pom updates for testing (#86)
* Make foreign key column not null (#82)
* Fix bug where new workflow definitions are not created

## [0.3.0] 2021-05-10

### Added
* Add better error context information
* [GP-2673] Niassa Migration WorkflowEngine & OutputProvisioner (#57)
* Add more indices
* add Git Code Format Maven Plugin (#65)
* Add names to pom files

### Changed
* Redesign provider interfaces
* Redesign add workflow version endpoint
* Always use jOOQ transaction layer

### Fixed
* Check for duplicate external keys on load
* Fixes comparison bug in BaseProcessor (#75)
* Fixes bug on scheduling tasks from raw input (#74)
* Fix bug in output list handling

## [0.2.0] 2021-04-28

### Changed
* Add default constructor for InFlightValue
* Add classes used by Shesmu to deserialize max-in-flight data; use InFlightValue instead of Pair to store results
* formatting
* Open more packages to Jackson for reflection
* Move bulk version requests to public API package

## [0.1.3] 2021-03-25

### Added
* Add load/unload functionality
* Add DTOs for unloaded data
* Add URL field to analysis DTO
* Add Jackson `java.time` support
* Add externally-visible work contract to interface
* Add DB states directory with a DB state for workflow runs waiting on resources
* Endpoint for all waiting workflows
* Add Shesmu Style XML for IntelliJ, with documentation; apply Shesmu Style to MaxInFlight code changes
* Add max-in-flight endpoint to the Vidarr server

### Changed
* Update MIME types
* Upgrade server-utils to 1.0.4
* Autoformatting
* Rearrange code formatting
* Include attempt in operation DTO

### Fixed
* Unify workflow run hash computation
* Fix analysis DTO and endpoints to match
* Add missing database constraints
* Ensure that workflow run start time is written
* Make sure all type support equals
* Fix incorrect workflow run hash generation
* Force new workflow version to have parameters
* Provide better deserialization errors
* Fix contract bug in Cromwell output provisioner and workflow engine
* Fix analysis ID regexp
* force brackets (#46)

## [0.1.2] 2021-03-01

### Added
* Allow reattempting unstarted workflows
* Add output chunking for Cromwell provisioner
* Add documentation about Vidarr identifiers
* Add operation's phase to database
* Add `equals` and `hashCode` that ignore attempt

### Changed
* Remove unloader interface that needs redesign
* Move validateLabels to static context for future reuse
* Change `externalIds` to `externalKeys`
* Have failed operations trigger consumable resources release
* Append to array list rather than setting
* Show engine phase in human-readable format
* Log exceptions using logging infrastructure
* Allow null labels

### Fixed
* Fix provenance workflow run DTO
* Fix typo in JavaDoc
* Fix another NPE related to labels
* prepared for next development iteration

## [0.1.1] 2021-02-24

### Changed
* Change Cromwell engine parameter type parsing
* Apply automatic code cleanups
* Autoformatting
* Include default JVM Prometheus exports
* Refactor `DatabaseBackedProcessor.submit`
* Configure for deployment

### Fixed
* Fix accessory workflow file hash
* Fix bugs in input ID handling and hashing
* prepared for next development iteration

## [0.1.0] 2021-02-11

### Added
* Create consumable resource infrastructure
* Add DTOs for `/api/targets` and `/api/workflows`
* Add release script
* Add additional DTOs
* Setup GitHub Actions
* Add a guide to workflow types and fix errors
* Write documentation
* Create overall operation status
* Add `/api/status` endpoint to list all active workflows
* Add Niassa workflow language
* Add debug information from Cromwell
* Add support for debugging information to be exported
* Implement database layer and server
* Create workflow test infrastructure
* Create server infrastructure
* Initial skeleton

### Changed
* Allow multiple files in a workflow
* Allow deleting failed workflow runs
* Handle null during start up recovery
* Return more helpful errors on invalid workflow metadata
* Sort members and table columns
* Rearrange webservice API classes
* Build jOOQ classes using Maven Docker
* Remove tagged unions in output types
* Change engine arguments to be any JSON type
* Check for lack of engine parameters
* Handle engine parameters for Cromwell workflow engine
* Handle generic types in body handler
* Remove support for optional parameters
* Allow restarting failed workflows
* Allow bulk update of external IDs
* Set theme jekyll-theme-slate
* Serialise database writes
* Autoformatting
* Allow no child operations in workflow run phase
* Use Hikari connection pooling
* Allow use of raw workflow for Cromwell provision out

### Fixed
* Fix release script permissions
* Remove unused tables
* Fix erroneous operation status as `WAITING`
* Fix running status
