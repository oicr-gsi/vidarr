package ca.on.oicr.gsi.vidarr;

import tools.jackson.core.JsonGenerator;
import java.io.IOException;

interface Printer {
  void print(JsonGenerator jsonGenerator) throws IOException;
}
