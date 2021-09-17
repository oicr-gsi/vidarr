ALTER TABLE active_workflow_run ADD COLUMN real_input_index INTEGER NOT NULL DEFAULT 0;
UPDATE active_workflow_run SET real_input = json_build_array(real_input) WHERE real_input IS NOT NULL;
