package ca.on.oicr.gsi.vidarr;

final class PollResultFailed extends PollResult {

  private final String message;

  public PollResultFailed(String message) {
    super();
    this.message = message;
  }

  @Override
  public void visit(Visitor visitor) {
    visitor.failed(message);
  }
}
