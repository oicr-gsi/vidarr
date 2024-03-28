DELETE FROM active_operation WHERE workflow_run_id IN (SELECT active_workflow_run.id FROM active_workflow_run);
DELETE FROM external_id_version WHERE external_id_version.external_id_id IN (SELECT external_id.id FROM external_id WHERE external_id.workflow_run_id IN (SELECT active_workflow_run.id FROM active_workflow_run));
DELETE FROM analysis_external_id WHERE analysis_external_id.external_id_id IN (SELECT external_id.id FROM external_id WHERE external_id.workflow_run_id IN (SELECT active_workflow_run.id FROM active_workflow_run));
DELETE FROM external_id WHERE external_id.workflow_run_id IN (SELECT active_workflow_run.id FROM active_workflow_run);
DELETE FROM analysis WHERE analysis.workflow_run_id IN (SELECT active_workflow_run.id FROM active_workflow_run);
DELETE FROM active_workflow_run;
DELETE FROM workflow_run WHERE completed IS NULL;
