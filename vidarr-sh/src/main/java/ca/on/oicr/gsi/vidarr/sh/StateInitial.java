package ca.on.oicr.gsi.vidarr.sh;

import com.fasterxml.jackson.databind.JsonNode;

/** The shell workflow engine's state to be stored in the database */
public record StateInitial(String workflow, JsonNode input, boolean hasAccessoryFiles) {}
