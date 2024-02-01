import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.sh.UnixShellWorkflowEngine;

/**
 * A workflow manager that runs job as UNIX shell commands locally
 *
 * <p>This is for demonstration purposes and should not be used.
 */
module ca.on.oicr.gsi.vidarr.sh {
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires java.xml;

  opens ca.on.oicr.gsi.vidarr.sh to
      com.fasterxml.jackson.databind;

  provides WorkflowEngineProvider with
      UnixShellWorkflowEngine;
}
