package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.vidarr.ProvisionType;
import ca.on.oicr.gsi.vidarr.Provisioner;
import ca.on.oicr.gsi.vidarr.ProvisionerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;

public final class CromwellProvisioner implements Provisioner {
  public static ProvisionerProvider provider() {
    return new ProvisionerProvider() {
      @Override
      public Optional<Provisioner> readConfiguration(ObjectNode node) {
        // TODO
        return Optional.empty();
      }

      @Override
      public String type() {
        return "cromwell";
      }
    };
  }

  @Override
  public ProvisionType type() {
    return ProvisionType.FILES;
  }
}
