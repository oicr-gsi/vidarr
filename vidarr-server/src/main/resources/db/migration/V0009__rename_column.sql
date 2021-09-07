-- This change brings database in line with naming scheme in vidarr codebase, and is also more accurate and less confusing
ALTER TABLE active_workflow_run
RENAME COLUMN external_input_ids_handled TO extra_input_ids_handled;

-- I reordered the Phase enum, which has Database Consequences
UPDATE active_operation SET engine_phase = 5555 WHERE engine_phase = 6;
UPDATE active_operation SET engine_phase = 6 WHERE engine_phase = 7;
UPDATE active_operation SET engine_phase = 7 WHERE engine_phase = 5555;
UPDATE active_workflow_run SET engine_phase = 5555 WHERE engine_phase = 6;
UPDATE active_workflow_run SET engine_phase = 6 WHERE engine_phase = 7;
UPDATE active_workflow_run SET engine_phase = 7 WHERE engine_phase = 5555;