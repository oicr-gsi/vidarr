package ca.on.oicr.gsi.vidarr.core;

import tools.jackson.databind.JsonNode;
import java.util.List;

public record InputProvisioningStateExternal(List<JsonPath> mutation, JsonNode metadata) {}
