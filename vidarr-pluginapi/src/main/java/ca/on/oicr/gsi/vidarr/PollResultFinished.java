package ca.on.oicr.gsi.vidarr;

final class PollResultFinished extends PollResult {

  @Override
  public void visit(Visitor visitor) {
    visitor.finished();
  }
}
