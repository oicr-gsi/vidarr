CREATE OR REPLACE FUNCTION get_ids_for_downstream_workflow_runs(wr_ids BIGINT[]) RETURNS table (wfr_id bigint) AS $$
  BEGIN
    RETURN QUERY
    SELECT DISTINCT a.id FROM
      (SELECT id, SPLIT_PART(UNNEST(input_file_ids), '/', 3)::VARCHAR AS input_file_ids
      FROM workflow_run) AS a
    WHERE a.input_file_ids IN (SELECT DISTINCT hash_id FROM analysis WHERE workflow_run_id = ANY(wr_ids));
  END;
$$ LANGUAGE plpgsql;