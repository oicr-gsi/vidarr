package ca.on.oicr.gsi.vidarr.cromwell;

import java.net.URI;

/**
 * Heavily multithreaded Cromwell workflow runs have such a large 'calls' block that requesting it
 * repeatedly can lock up Cromwell. Preserve Cromwell performance by limiting when we request that
 * block to when we actually want to display that information. In production, that's only on Failed
 * workflow runs. In development, we'd like to see it while processing is happening. The Cromwell
 * WorkflowEngine and OutputProvisioner use configuration to control this behaviour.
 */
final class CromwellMetadataURL {
  protected static URI formatMetadataURL(String rootUrl, String cromwellId, boolean includeCalls) {
    String metadataAPIformat = "%s/api/workflows/v1/%s/metadata";

    // Note that the URL specifies to *exclude* keys, hence the inverted bool test
    if (!includeCalls)
      metadataAPIformat += "?excludeKey=calls&excludeKey=submittedFiles&expandSubWorkflows=false";

    return URI.create(String.format(metadataAPIformat, rootUrl, cromwellId));
  }
}
