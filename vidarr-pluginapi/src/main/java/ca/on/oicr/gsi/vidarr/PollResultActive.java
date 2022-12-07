package ca.on.oicr.gsi.vidarr;

final class PollResultActive extends PollResult {

  private final WorkingStatus status;

  public PollResultActive(WorkingStatus status) {
    super();
    this.status = status;
  }

  @Override
  public void visit(Visitor visitor) {
    visitor.active(status);
  }
}
