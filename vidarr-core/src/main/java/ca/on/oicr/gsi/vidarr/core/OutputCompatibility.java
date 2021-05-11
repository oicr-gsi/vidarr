package ca.on.oicr.gsi.vidarr.core;

public enum OutputCompatibility {
  INDIFFERENT,
  OPTIONAL_WITH_MANUAL,
  MANDATORY_WITH_REMAINING,
  BROKEN;

  public OutputCompatibility worst(OutputCompatibility other) {
    switch (this) {
      case BROKEN:
        return BROKEN;
      case INDIFFERENT:
        return other;
      case OPTIONAL_WITH_MANUAL:
        switch (other) {
          case BROKEN:
          case MANDATORY_WITH_REMAINING:
            return BROKEN;
          default:
            return this;
        }
      case MANDATORY_WITH_REMAINING:
        switch (other) {
          case BROKEN:
          case OPTIONAL_WITH_MANUAL:
            return BROKEN;
          default:
            return this;
        }
    }
    return BROKEN;
  }
}
