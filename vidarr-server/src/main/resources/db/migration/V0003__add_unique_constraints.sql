ALTER TABLE active_workflow_run ADD CONSTRAINT active_Workflow_run_id_fkey FOREIGN KEY (id) REFERENCES workflow_run (id);
ALTER TABLE external_id ADD CONSTRAINT external_id_provider_id_unique UNIQUE (workflow_run_id, provider, external_id);
ALTER TABLE external_id_version ADD CONSTRAINT external_id_key_value_unique UNIQUE(external_id_id, key, value);
ALTER TABLE workflow_definition ADD CONSTRAINT workflow_definition_hash_unique UNIQUE(hash_id);
ALTER TABLE workflow_definition ADD CONSTRAINT workflow_definition_id_pkey PRIMARY KEY(id);
ALTER TABLE workflow_version ADD CONSTRAINT workflow_version_definition_id_fkey FOREIGN KEY (workflow_definition) REFERENCES workflow_definition(id);
ALTER TABLE workflow_version ADD CONSTRAINT workflow_version_name_unique UNIQUE (name, version);
ALTER TABLE workflow_version_accessory ADD CONSTRAINT workflow_definition_id_fkey FOREIGN KEY (workflow_definition) REFERENCES workflow_definition(id);
ALTER TABLE workflow_version_accessory ADD CONSTRAINT workflow_version_id_fkey FOREIGN KEY (workflow_version) REFERENCES workflow_version(id);
