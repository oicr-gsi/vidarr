CREATE INDEX analysis_workflow_run ON analysis(workflow_run_id);
CREATE INDEX external_id_workflow_run ON external_id(workflow_run_id);
CREATE INDEX workflow_run_completed ON workflow_run(completed);
CREATE INDEX workflow_run_version ON workflow_run(workflow_version_id);
