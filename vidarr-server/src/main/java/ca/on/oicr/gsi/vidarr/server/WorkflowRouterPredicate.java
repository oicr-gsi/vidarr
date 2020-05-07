package ca.on.oicr.gsi.vidarr.server;

import ca.on.oicr.gsi.status.SectionRenderer;
import javax.xml.stream.XMLStreamException;

public interface WorkflowRouterPredicate {
  void display(SectionRenderer sectionRenderer) throws XMLStreamException;
}
