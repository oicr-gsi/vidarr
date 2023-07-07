package ca.on.oicr.gsi.vidarr.api;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Class to record priority by workflow */
public class PriorityByWorkflow {

  private Map<String, Integer> counts = new ConcurrentHashMap<>();

  public void add(String workflow, Integer priority) {
    counts.put(workflow, priority);
  }

  public int getPriority(String workflow) {
    return counts.get(workflow);
  }

  public Set<String> getWorkflows() {
    return counts.keySet();
  }
}
