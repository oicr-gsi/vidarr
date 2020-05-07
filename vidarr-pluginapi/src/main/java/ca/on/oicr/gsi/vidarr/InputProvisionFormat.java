package ca.on.oicr.gsi.vidarr;

/** The type of data the provision is expecting to provide to the workflow */
public enum InputProvisionFormat {
  /** The provisioner can handle individual files */
  FILE(InputType.FILE),
  /** The provisioner can handle directories */
  DIRECTORY(InputType.DIRECTORY);
  private final InputType type;

  InputProvisionFormat(InputType type) {
    this.type = type;
  }

  /** The input type associated with this format */
  public InputType type() {
    return type;
  }
}
