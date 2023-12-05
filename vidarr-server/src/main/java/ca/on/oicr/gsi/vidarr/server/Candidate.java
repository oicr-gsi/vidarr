package ca.on.oicr.gsi.vidarr.server;

import java.time.Instant;

record Candidate(long id, String workflowRun, Instant created) {}
