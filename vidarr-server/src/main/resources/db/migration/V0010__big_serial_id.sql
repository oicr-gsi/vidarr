ALTER TABLE workflow_run ALTER COLUMN id TYPE bigint;
ALTER TABLE active_workflow_run ALTER COLUMN id TYPE bigint;
ALTER TABLE active_operation ALTER COLUMN workflow_run_id TYPE bigint;
ALTER TABLE external_id ALTER COLUMN workflow_run_id TYPE bigint;
ALTER TABLE analysis ALTER COLUMN workflow_run_id TYPE bigint;