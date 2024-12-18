package ca.on.oicr.gsi.vidarr.server;

public class ValidationError {
  public static final String GENERAL_PROPERTY = "GENERAL";
  private static final String DEFAULT_PROPERTY_MESSAGE = "This value is invalid";
  private static final String DEFAULT_GENERAL_MESSAGE = "This data is invalid";

  private final String property;
  private final String message;

  /**
   * Constructs a ValidationError to describe one of possibly several reasons that an object is
   * invalid
   *
   * @param property name of the invalid property; set to null if it applies to no particular
   *     property
   * @param message user-friendly description of invalidity
   */
  public ValidationError(String property, String message) {
    this.property = property == null ? GENERAL_PROPERTY : property;
    if (message != null) {
      this.message = message;
    } else {
      this.message = property == null ? DEFAULT_GENERAL_MESSAGE : DEFAULT_PROPERTY_MESSAGE;
    }
  }

  public static ValidationError forRequired(String property) {
    return new ValidationError(property, "This field is required");
  }

  /**
   * Constructs a ValidationError to describe one of possibly several reasons that an object is
   * invalid. If the error applies to a specific field, a different constructor should be used
   *
   * @param message user-friendly description of invalidity
   */
  public ValidationError(String message) {
    this(null, message);
  }

  /**
   * @return name of invalid property, or "GENERAL" if the error applies to no particular property
   */
  public String getProperty() {
    return property;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return String.format("%s: %s.", getProperty(), getMessage());
  }
}
