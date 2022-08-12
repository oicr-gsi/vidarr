package ca.on.oicr.gsi.vidarr.cromwell;

import ca.on.oicr.gsi.vidarr.LogFileStasher.Kind;
import java.util.Map;

public final class CallLogState {
  private int index;
  private Kind kind;
  private Map<String, String> labels;
  private String log;

  public int getIndex() {
    return index;
  }

  public Kind getKind() {
    return kind;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public String getLog() {
    return log;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public void setKind(Kind kind) {
    this.kind = kind;
  }

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }

  public void setLog(String log) {
    this.log = log;
  }
}
