package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.Pair;
import ca.on.oicr.gsi.vidarr.ConsumableResource;
import ca.on.oicr.gsi.vidarr.InputProvisionFormat;
import ca.on.oicr.gsi.vidarr.InputProvisioner;
import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;
import ca.on.oicr.gsi.vidarr.OutputProvisioner;
import ca.on.oicr.gsi.vidarr.RuntimeProvisioner;
import ca.on.oicr.gsi.vidarr.WorkflowEngine;
import java.util.stream.Stream;

/** The workflow engine configuration for the processor to use */
public interface Target {

  /** All consumable resources */
  Stream<Pair<String, ConsumableResource>> consumableResources();
  /** The workflow engine plugin to use */
  WorkflowEngine<?, ?> engine();

  /**
   * The input provisioner plugins to use
   *
   * @param type the format being provisioned in
   */
  InputProvisioner<?> provisionerFor(InputProvisionFormat type);

  /**
   * The output provision plugins to use
   *
   * @param type the format being provisioned out
   */
  OutputProvisioner<?, ?> provisionerFor(OutputProvisionFormat type);

  /** Any runtime provisioners to use on every workflow run */
  Stream<RuntimeProvisioner<?>> runtimeProvisioners();
}
