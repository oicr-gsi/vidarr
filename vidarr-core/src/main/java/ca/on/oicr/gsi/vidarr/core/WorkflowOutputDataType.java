package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.OutputProvisionFormat;

/** The output data that will be provided by the workflow */
public enum WorkflowOutputDataType {
  DATAWAREHOUSE_RECORDS(OutputProvisionFormat.DATAWAREHOUSE_RECORDS),
  FILE(OutputProvisionFormat.FILES),
  FILE_WITH_LABELS(OutputProvisionFormat.FILES),
  FILES(OutputProvisionFormat.FILES),
  FILES_WITH_LABELS(OutputProvisionFormat.FILES),
  LOGS(OutputProvisionFormat.LOGS),
  QUALITY_CONTROL(OutputProvisionFormat.QUALITY_CONTROL);

  private final OutputProvisionFormat format;

  WorkflowOutputDataType(OutputProvisionFormat format) {
    this.format = format;
  }

  /** The provisioning plugin type required */
  public OutputProvisionFormat format() {
    return format;
  }
}
