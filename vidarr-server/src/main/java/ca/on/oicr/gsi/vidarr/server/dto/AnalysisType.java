package ca.on.oicr.gsi.vidarr.server.dto;

public enum AnalysisType {
  FILE {
    @Override
    public String dbType() {
      return "file";
    }
  },
  URL {
    @Override
    public String dbType() {
      return "url";
    }
  };

  public abstract String dbType();
}
