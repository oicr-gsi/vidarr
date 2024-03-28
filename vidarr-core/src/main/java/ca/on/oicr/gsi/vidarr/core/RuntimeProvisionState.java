package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import java.util.Map;
import java.util.Set;

/** The accessory data that must be stored during output provisioning */
public record RuntimeProvisionState(
    Set<? extends ExternalId> ids, Map<String, String> labels, String workflowRunUrl) {}
