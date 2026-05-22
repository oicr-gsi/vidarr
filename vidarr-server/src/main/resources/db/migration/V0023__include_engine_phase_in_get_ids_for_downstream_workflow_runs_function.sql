-- drop and recreate the function with a different return type
DROP FUNCTION get_ids_for_downstream_workflow_runs(bigint[]);
CREATE OR REPLACE FUNCTION get_ids_for_downstream_workflow_runs(wr_ids BIGINT[]) RETURNS table (wfr_id bigint, engine_phase int) AS $$
BEGIN
RETURN QUERY
SELECT DISTINCT a.id, awr.engine_phase FROM
    (SELECT id, SPLIT_PART(UNNEST(input_file_ids), '/', 3)::VARCHAR AS input_file_ids
     FROM workflow_run) AS a
        LEFT JOIN active_workflow_run awr ON a.id = awr.id
WHERE a.input_file_ids IN (SELECT DISTINCT hash_id FROM analysis WHERE workflow_run_id = ANY(wr_ids));
END;
$$ LANGUAGE plpgsql;