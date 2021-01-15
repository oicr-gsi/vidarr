ALTER TABLE active_operation ADD COLUMN debug_info jsonb NOT NULL DEFAULT 'null'::jsonb;

ALTER TABLE active_workflow_run ADD COLUMN attempt integer NOT NULL DEFAULT 0;
ALTER TABLE active_operation ADD COLUMN attempt integer NOT NULL DEFAULT 0;
