package ca.on.oicr.gsi.vidarr.core;

import ca.on.oicr.gsi.vidarr.PriorityScorer;
import io.undertow.Handlers;
import io.undertow.server.HttpHandler;
import java.util.List;
import java.util.Optional;

public abstract class BaseAggregatePriorityScorer implements PriorityScorer {
  protected List<PriorityScorer> scorers;

  public final List<PriorityScorer> getScorers() {
    return scorers;
  }

  @Override
  public final Optional<HttpHandler> httpHandler() {
    final var routes = Handlers.path();
    for (var i = 0; i < scorers.size(); i++) {
      final var prefix = "/" + i;
      scorers.get(i).httpHandler().ifPresent(h -> routes.addPrefixPath(prefix, h));
    }
    return Optional.of(routes);
  }

  @Override
  public final void recover(String workflowName, String workflowVersion, String vidarrId) {
    for (final var scorer : scorers) {
      scorer.recover(workflowName, workflowVersion, vidarrId);
    }
  }

  @Override
  public final void release(String workflowName, String workflowVersion, String vidarrId) {
    for (final var scorer : scorers) {
      scorer.release(workflowName, workflowVersion, vidarrId);
    }
  }

  public final void setScorers(List<PriorityScorer> scorers) {
    this.scorers = scorers;
  }

  @Override
  public final void startup() {
    for (final var scorer : scorers) {
      scorer.startup();
    }
    startupInner();
  }

  protected abstract void startupInner();
}
