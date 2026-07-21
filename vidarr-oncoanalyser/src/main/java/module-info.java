import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.oncoanalyser.OncoAnalyserWorkflowEngine;

module vidarr.oncoanalyser {
    requires ca.on.oicr.gsi.vidarr.pluginapi;
    requires java.xml;
    requires tools.jackson.databind;

    provides WorkflowEngineProvider
            with OncoAnalyserWorkflowEngine;
}