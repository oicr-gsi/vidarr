-- The input file ids are Vidarr IDs formatted like: vidarr:<server>/file/<hash>
-- Unnest the array of Vidarr IDs, then split each Vidarr ID on the slash and select the third item (<hash>)

    CREATE OR REPLACE FUNCTION is_downstream_from(wr_ids BIGINT[]) RETURNS table (wfr_id bigint) AS $$
      BEGIN
        RETURN QUERY
        SELECT DISTINCT id FROM workflow_run
                WHERE array_length(input_file_ids, 1) > 0 AND
                  -- workflow uses files produced by target workflow run as its input
                  (SELECT ARRAY(SELECT DISTINCT SPLIT_PART(UNNEST(input_file_ids), '/', 3)::VARCHAR FROM workflow_run)) <@ (
                    SELECT ARRAY(SELECT DISTINCT hash_id FROM analysis WHERE ARRAY[workflow_run_id] <@ wr_ids));
      END;
      $$ LANGUAGE plpgsql;