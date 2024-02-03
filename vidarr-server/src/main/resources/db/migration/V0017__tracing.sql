ALTER TABLE active_workflow_run ADD COLUMN tracing jsonb NOT NULL DEFAULT '{}'::jsonb;
