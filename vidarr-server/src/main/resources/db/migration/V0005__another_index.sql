SELECT id INTO TEMP TABLE wfv_to_delete
  FROM workflow_version WHERE workflow_definition IS NULL;

DELETE FROM analysis_external_id
  WHERE analysis_id IN
    (SELECT id FROM analysis WHERE workflow_run_id IN
      (SELECT id FROM workflow_run WHERE workflow_version_id IN
        (SELECT id FROM wfv_to_delete)));
DELETE FROM analysis
  WHERE workflow_run_id IN
    (SELECT id FROM workflow_run WHERE workflow_version_id IN
      (SELECT id FROM wfv_to_delete));
DELETE FROM active_operation
  WHERE workflow_run_id IN
    (SELECT id FROM workflow_run WHERE workflow_version_id IN
      (SELECT id FROM wfv_to_delete));
DELETE FROM active_workflow_run
  WHERE id IN
    (SELECT id FROM workflow_run WHERE workflow_version_id IN
      (SELECT id FROM wfv_to_delete));
DELETE FROM workflow_run WHERE workflow_version_id IN
  (SELECT id FROM wfv_to_delete);
DELETE FROM workflow_version_accessory WHERE workflow_version IN
  (SELECT id FROM wfv_to_delete);
DELETE FROM workflow_version WHERE workflow_definition IS NULL;

ALTER TABLE workflow_version ALTER COLUMN workflow_definition SET NOT NULL;
