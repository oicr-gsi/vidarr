import ca.on.oicr.gsi.vidarr.WorkflowEngineProvider;
import ca.on.oicr.gsi.vidarr.oncoanalyser.OncoAnalyserWorkflowEngine;

module vidarr.oncoanalyser {
    requires ca.on.oicr.gsi.vidarr.pluginapi;
    requires java.xml;
    requires tools.jackson.databind;
    requires com.fasterxml.jackson.annotation;

    provides WorkflowEngineProvider
            with OncoAnalyserWorkflowEngine;

    opens ca.on.oicr.gsi.vidarr.oncoanalyser.submission to tools.jackson.databind;
}