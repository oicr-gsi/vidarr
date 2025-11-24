package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OutputProvisioner.Result;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import java.util.Map;
import java.util.Set;

public record ProvisionData(
    Set<? extends ExternalId> ids, Map<String, String> labels, Result result, String data) {}
