package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.api.ExternalId;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Set;

/** The accessory data that must be stored during output provisioning */
public record OutputProvisionState(
    Set<? extends ExternalId> ids,
    Map<String, String> labels,
    String workflowRunId,
    String data,
    JsonNode metadata) {}
