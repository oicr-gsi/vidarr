-- index deployed for testing
DROP INDEX IF EXISTS external_id_provider_external_id;

CREATE INDEX ON external_id (provider);
CREATE INDEX ON external_id (external_id);
