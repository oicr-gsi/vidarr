package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.status.SectionRenderer;
import javax.xml.stream.XMLStreamException;

public interface WorkflowRouter {

  WorkflowRouter DENY =
      new WorkflowRouter() {
        @Override
        public void display(SectionRenderer sectionRenderer) throws XMLStreamException {
          sectionRenderer.line("Condition", "All");
          sectionRenderer.line("", "Deny");
        }
      };

  static WorkflowRouter then(WorkflowRouter first, WorkflowRouter second) {
    return new WorkflowRouter() {
      @Override
      public void display(SectionRenderer sectionRenderer) throws XMLStreamException {
        first.display(sectionRenderer);
        second.display(sectionRenderer);
      }
    };
  }

  void display(SectionRenderer sectionRenderer) throws XMLStreamException;
}
