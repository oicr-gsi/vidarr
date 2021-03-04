package ca.on.oicr.gsi.vidarr.api;

import ca.on.oicr.gsi.Pair;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class to record current and maximum inflight counts by workflow
 */
public class InFlightCountsByWorkflow {

  private Map<String, Pair<Integer, Integer>> counts = new ConcurrentHashMap<>();

  public void add(String workflow, Integer current, Integer max) {
    Pair values = new Pair(current, max);
    counts.put(workflow, values);
  }

  public int getCurrent(String workflow) {
    return counts.get(workflow).first();
  }

  public int getMax(String workflow) {
    return counts.get(workflow).second();
  }

  public Set<String> getWorkflows() {
    return counts.keySet();
  }

}
