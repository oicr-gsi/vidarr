package ca.on.oicr.gsi.vidarr.api;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Class to record current and maximum inflight counts by workflow */
public class InFlightCountsByWorkflow {

  private Map<String, InFlightValue> counts = new ConcurrentHashMap<>();

  public void add(String workflow, Integer current, Integer max) {
    counts.put(workflow, new InFlightValue(current, max));
  }

  public int getCurrent(String workflow) {
    return counts.get(workflow).getCurrentInFlight();
  }

  public int getMax(String workflow) {
    return counts.get(workflow).getMaxInFlight();
  }

  public Set<String> getWorkflows() {
    return counts.keySet();
  }
}
