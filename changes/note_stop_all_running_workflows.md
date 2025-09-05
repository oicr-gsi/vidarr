The new HTTP handling step is used in the CromwellWorkflowEngine and CromwellOutputProvisioner and these changes are not compatible with existing workflow runs.

Any existing workflow runs that were in RUNNING or PROVISION_OUT will be unrecoverable once this upgrade completes.
