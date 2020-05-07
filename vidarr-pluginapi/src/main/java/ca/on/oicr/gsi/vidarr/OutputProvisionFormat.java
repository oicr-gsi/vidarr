package ca.on.oicr.gsi.vidarr;

/** The type of data the provision is expecting to receive */
public enum OutputProvisionFormat {
  /** The provisioner can handle structured records that can go in a data warehouse */
  DATAWAREHOUSE_RECORDS(OutputProvisionType.WAREHOUSE_RECORDS),
  /** The provisioner can handle file metadata */
  FILES(OutputProvisionType.FILES),
  /**
   * The provisioner can handle logs (text files that are for debugging purposes, not further
   * analysis)
   */
  LOGS(OutputProvisionType.LOGS),
  /** The provisioner can handle quality control metadata */
  QUALITY_CONTROL(OutputProvisionType.QUALITY_CONTROL);
  private final OutputProvisionType outputType;

  OutputProvisionFormat(OutputProvisionType outputType) {
    this.outputType = outputType;
  }

  /** The type of output data that must be provided for this provisioner */
  public OutputProvisionType outputType() {
    return outputType;
  }
}
