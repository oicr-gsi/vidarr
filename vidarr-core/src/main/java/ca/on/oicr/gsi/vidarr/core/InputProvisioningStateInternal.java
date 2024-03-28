package ca.on.oicr.gsi.vidarr.core;

import java.util.List;

public record InputProvisioningStateInternal(List<JsonPath> mutation, String id, String path) {}
