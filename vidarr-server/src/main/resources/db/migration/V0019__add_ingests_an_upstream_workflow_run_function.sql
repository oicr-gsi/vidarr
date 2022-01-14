-- This function takes the workflow_run.input_file_ids varchar[] column as input.
-- The input file ids are Vidarr IDs formatted like: vidarr:<server>/file/<hash>
-- Unnest the array of Vidarr IDs, then split each Vidarr ID on the slash and select the third item (<hash>)
--   (sometimes there will be multiple values for <server> within a single database)

   CREATE OR REPLACE FUNCTION ingests_an_upstream_workflow_run(workflow_run_ids BIGINT[]) RETURNS bigint[] AS $$
     BEGIN
      SELECT workflow_run_id FROM analysis
        WHERE
          -- workflow run ID is present in target list
          workflow_run_id = ANY (workflow_run_ids)
        AND
          -- file hash is used as an input to a workflow run
          ARRAY[hash_id] <@ (
            SELECT ARRAY(SELECT DISTINCT SPLIT_PART(UNNEST(input_file_ids), '/', 3)::VARCHAR FROM workflow_run));
      END;
      $$ LANGUAGE plpgsql;

--    CREATE OR REPLACE FUNCTION select_upstream_files(workflow_run_ids BIGINT[]) RETURNS analysis AS $$
--         BEGIN
--          SELECT * FROM analysis
--            WHERE workflow_run_id = ANY (workflow_run_ids)
--            AND ARRAY[hash_id] <@ (
--                SELECT DISTINCT SPLIT_PART(file_ids.fid::VARCHAR, '/', 3)
--                  FROM workflow_run, LATERAL (SELECT DISTINCT fid FROM UNNEST(input_file_ids) AS ifi(fid)) file_ids);
--          END;
--          $$ LANGUAGE plpgsql;

      --AS $$ SELECT SPLIT_PART(input_file_ids.*, '/', 3)::VARCHAR FROM workflow_run, LATERAL UNNEST($1) AS input_file_ids(ifi) $$

   --   AS $$ SELECT hash_id FROM analysis
   --     WHERE workflow_run_id IN $1
   --     AND $2 <@ (
   --         SELECT ARRAY(SELECT DISTINCT(
   --           SELECT SPLIT_PART(input_file_ids.*, '/', 3)::VARCHAR
   --           FROM workflow_run, LATERAL UNNLEST(input_file_ids) AS input_file_ids(ifi))
   --         FROM workflow_run) $$
