DROP INDEX IF EXISTS external_id_provider_external_id;
CREATE INDEX external_id_provider_external_id ON external_id (provider, external_id);

