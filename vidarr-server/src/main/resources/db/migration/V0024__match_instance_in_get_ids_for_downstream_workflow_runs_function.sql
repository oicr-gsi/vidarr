-- drop and recreate the function so input files only match when they refer to this instance
DROP FUNCTION get_ids_for_downstream_workflow_runs(bigint[]);
CREATE OR REPLACE FUNCTION get_ids_for_downstream_workflow_runs(wr_ids BIGINT[], instance_name VARCHAR) RETURNS table (wfr_id bigint, engine_phase int) AS $$
BEGIN
RETURN QUERY
SELECT DISTINCT a.id, awr.engine_phase FROM
    (SELECT id, UNNEST(input_file_ids) AS input_file_id
     FROM workflow_run) AS a
        LEFT JOIN active_workflow_run awr ON a.id = awr.id
WHERE a.input_file_id IN (SELECT DISTINCT 'vidarr:' || instance_alias || '/file/' || hash_id
    FROM analysis, UNNEST(ARRAY['_', instance_name]) AS instance_alias
    WHERE workflow_run_id = ANY(wr_ids));
    -- this code:
    -- `WHERE a.input_file_id IN (...) AS instance_alias WHERE workflow_run_id = ANY(wr.ids)`
    -- is equivalent to (but more efficient than):
    -- WHERE a.input_file_id IN (
    -- SELECT 'vidarr:_/file/' || hash_id FROM analysis WHERE workflow_run_id = ANY(wr_ids)
    -- UNION
    -- SELECT 'vidarr:' || instance_name || '/file/' || hash_id FROM analysis WHERE workflow_run_id = ANY(wr_ids))
END;
$$ LANGUAGE plpgsql;
