import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.sh.UnixShellWorkflowEngine;

/**
 * A workflow manager that runs job as UNIX shell commands locally
 *
 * <p>This is for demonstration purposes and should not be used.
 */
module ca.on.oicr.gsi.vidarr.sh {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires tools.jackson.core;
  requires tools.jackson.databind;
  requires java.xml;

  opens ca.on.oicr.gsi.vidarr.sh to
      tools.jackson.databind;

  provides WorkflowEngineProvider with
      UnixShellWorkflowEngine;
}
