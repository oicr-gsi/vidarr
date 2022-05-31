CREATE INDEX IF NOT EXISTS niassa_workflow_run_swid_index ON workflow_run ((arguments ->> 'workflowRunSWID'));
