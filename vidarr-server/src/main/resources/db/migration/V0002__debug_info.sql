ALTER TABLE active_operation ADD COLUMN debug_info jsonb NOT NULL DEFAULT 'null'::jsonb;
