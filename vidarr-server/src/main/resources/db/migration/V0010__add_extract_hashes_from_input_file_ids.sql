-- This function takes the workflow_run.input_file_ids varchar[] column as input.
-- The input file ids are Vidarr IDs formatted like: vidarr:<server>/file/<hash>
-- Unnest the array of Vidarr IDs, then split each Vidarr ID on the slash and select the third item (<hash>)
--   (sometimes there will be multiple values for <server> within a single database)
CREATE OR REPLACE FUNCTION extract_hashes_from_input_file_ids(input_file_ids VARCHAR[]) RETURNS VARCHAR
   AS $$ SELECT SPLIT_PART(UNNEST($1), '/', 3)::VARCHAR $$
   LANGUAGE SQL;
