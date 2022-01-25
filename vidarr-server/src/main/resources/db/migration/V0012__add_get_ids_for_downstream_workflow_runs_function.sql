-- The input file ids are Vidarr IDs formatted like: vidarr:<server>/file/<hash>
-- Unnest the array of Vidarr IDs, then split each Vidarr ID on the slash and select the third item (<hash>)

    CREATE OR REPLACE FUNCTION get_ids_for_downstream_workflow_runs(wr_ids BIGINT[]) RETURNS table (wfr_id bigint) AS $$
      BEGIN
        RETURN QUERY
        SELECT DISTINCT a.id FROM
          (SELECT id, ARRAY(SELECT SPLIT_PART(UNNEST(input_file_ids), '/', 3)::VARCHAR) AS input_file_ids
          FROM workflow_run) AS a
        WHERE cardinality(a.input_file_ids) > 0 AND a.input_file_ids && (
          SELECT ARRAY(SELECT DISTINCT hash_id FROM analysis WHERE ARRAY[workflow_run_id] <@ wr_ids));
          -- note: the `&&` is the array intersection operator, whereas `[A] <@ [B]` is "all elements in A are present in B"
      END;
      $$ LANGUAGE plpgsql;