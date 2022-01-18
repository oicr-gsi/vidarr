-- This function takes the workflow_run.input_file_ids varchar[] column as input.
-- The input file ids are Vidarr IDs formatted like: vidarr:<server>/file/<hash>
-- Unnest the array of Vidarr IDs, then split each Vidarr ID on the slash and select the third item (<hash>)
--   (sometimes there will be multiple values for <server> within a single database)

--   CREATE OR REPLACE FUNCTION is_upstream_workflow_run(workflow_run_ids BIGINT[]) RETURNS table (wfr_id bigint) AS $$
--     BEGIN
--      RETURN QUERY
--      SELECT DISTINCT workflow_run_id FROM analysis
--        WHERE
--          -- workflow run ID is present in target list
--          workflow_run_id = ANY (workflow_run_ids)
--        AND
--          -- file hash is used as an input to a workflow run
--          ARRAY[hash_id] <@ (
--            SELECT ARRAY(SELECT DISTINCT SPLIT_PART(UNNEST(input_file_ids), '/', 3)::VARCHAR FROM workflow_run));
--      END;
--      $$ LANGUAGE plpgsql;

    CREATE OR REPLACE FUNCTION is_downstream_from(wrids BIGINT[]) RETURNS table (wfr_id bigint) AS $$
      BEGIN
        RETURN QUERY
        SELECT DISTINCT id FROM workflow_run
                WHERE array_length(input_file_ids, 1) > 0 AND
                  -- workflow uses files produced by target workflow run as its input
                  (SELECT ARRAY(SELECT DISTINCT SPLIT_PART(UNNEST(input_file_ids), '/', 3)::VARCHAR FROM workflow_run)) <@ (
                    SELECT ARRAY(SELECT DISTINCT hash_id FROM analysis WHERE ARRAY[workflow_run_id] <@ wrids));
      END;
      $$ LANGUAGE plpgsql;