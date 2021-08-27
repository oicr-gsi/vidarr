-- This change brings database in line with naming scheme in vidarr codebase, and is also more accurate and less confusing
ALTER TABLE active_workflow_run
RENAME COLUMN external_input_ids_handled TO extra_input_ids_handled;