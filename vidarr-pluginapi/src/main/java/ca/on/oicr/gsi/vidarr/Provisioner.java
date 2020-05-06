package ca.on.oicr.gsi.vidarr;

/** A mechanism to collect output data from a workflow and push it into an appropriate data store */
public interface Provisioner {

  /** The type of data being provisioned */
  ProvisionType type();
}
