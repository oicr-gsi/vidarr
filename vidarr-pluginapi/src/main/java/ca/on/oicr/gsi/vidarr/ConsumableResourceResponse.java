package ca.on.oicr.gsi.vidarr;

/** The result from a request to access a consumable resource */
public abstract class ConsumableResourceResponse {

  /**
   * Transform the result of a consumable resource response into another value
   *
   * @param <T> the type to convert to
   */
  public interface Visitor<T> {

    /**
     * Convert the successful acquisition of a response
     *
     * @return the transformed object
     */
    T available();

    /**
     * Convert an unsuccessful response
     *
     * @param message the error message for why the resource could not be allocated
     * @return the transformed object
     */
    T error(String message);

    /**
     * Convert an unsuccessful but empty response
     *
     * @return the transformed object
     */
    T unavailable();
  }

  /**
   * An indication that this resource is fine to allow the workflow to be run and, at the end of the
   * workflow, the resources used do not need to be accounted for.
   */
  public static final ConsumableResourceResponse AVAILABLE =
      new ConsumableResourceResponse() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.available();
        }
      };
  /** A value that indicates the resource has insufficient capacity to fulfill the request. */
  public static final ConsumableResourceResponse UNAVAILABLE =
      new ConsumableResourceResponse() {
        @Override
        public <T> T apply(Visitor<T> visitor) {
          return visitor.unavailable();
        }
      };
  /**
   * Create a new error response
   *
   * <p>An error occurs when the input request is invalid or the resource's state cannot be safely
   * determined.
   *
   * @param message a message that should be reported to the user
   * @return a result value that can be provided to the resource-accessor
   */
  public static ConsumableResourceResponse error(String message) {
    return new ConsumableResourceResponse() {
      @Override
      public <T> T apply(Visitor<T> visitor) {
        return visitor.error(message);
      }
    };
  }

  private ConsumableResourceResponse() {}

  /**
   * Convert this response into another value
   *
   * @param visitor the converter
   * @param <T> the result type
   * @return the value from conversion
   */
  public abstract <T> T apply(Visitor<T> visitor);
}
