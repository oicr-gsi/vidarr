package ca.on.oicr.gsi.vidarr.core;

import static ca.on.oicr.gsi.vidarr.OperationAction.load;
import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.vidarr.BasicType;
import ca.on.oicr.gsi.vidarr.OperationAction;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionerProvider;
import ca.on.oicr.gsi.vidarr.core.LocalOutputProvisioner.ProvisionState;
import ca.on.oicr.gsi.vidarr.core.LocalOutputProvisioner.PreflightState;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import javax.xml.stream.XMLStreamException;

public class LocalOutputProvisioner implements OutputProvisioner<PreflightState, ProvisionState> {

  public record PreflightState(){}
  public record ProvisionState (String inputFile,
                               JsonNode metadata,
                               String vidarrId){
    public OutputProvisioner.Result move() throws IOException {
      final Path inputPath = Paths.get(inputFile),
          outputPath = Paths.get(metadata.get("outputDirectory").asText()),
          newPath = Files.move(inputPath, outputPath.resolve(inputPath.getFileName()), StandardCopyOption.REPLACE_EXISTING);
      // Calculate CRC32
      byte[] data = Files.readAllBytes(newPath);
      CRC32 crc32 = new CRC32();
      crc32.update(data);

      return Result.file(
          newPath.toString(),
          Long.toString(crc32.getValue()),
          "crc32",
          Files.size(newPath),
          "text/plain"
      );
    }
  }

  public static OutputProvisionerProvider provider() {
    return () -> Stream.of(new Pair<>("local", LocalOutputProvisioner.class));
  }

  @Override
  public boolean canProvision(OutputProvisionFormat format) {
    return format.equals(OutputProvisionFormat.FILES);
  }

  @Override
  public void configuration(SectionRenderer sectionRenderer) throws XMLStreamException {
    sectionRenderer.line("Local Output Provisioner", "provisions output locally");
  }

  @Override
  public PreflightState preflightCheck(JsonNode metadata) {
    return new PreflightState();
  }

  @Override
  public ProvisionState prepareProvisionInput(String workflowRunId, String data, JsonNode metadata) {
    return new ProvisionState(data, metadata, workflowRunId);
  }

  @Override
  public OperationAction<?, ProvisionState, Result> build() {
    // Use the ProvisionState to move a file from the inputFile to the outputDirectory
    return load(ProvisionState.class, ProvisionState::move);
  }

  @Override
  public OperationAction<?, PreflightState, Boolean> buildPreflight() {
    return OperationAction.value(PreflightState.class, true);
  }

  @Override
  public void startup() {
  }

  @Override
  public String type() {
    return "local";
  }

  @Override
  public BasicType typeFor(OutputProvisionFormat format) {
    if (format == OutputProvisionFormat.FILES){
      return BasicType.object(new Pair<>("outputDirectory", BasicType.STRING));
    } else {
      throw new IllegalArgumentException("Cannot provision types other than files");
    }
  }
}
