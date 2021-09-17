package ca.on.oicr.gsi.vidarr;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;

interface Printer {
  void print(JsonGenerator jsonGenerator) throws IOException;
}
