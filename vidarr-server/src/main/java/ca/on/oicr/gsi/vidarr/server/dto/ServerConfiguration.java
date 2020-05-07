package ca.on.oicr.gsi.vidarr.server.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class ServerConfiguration {
  private String dbHost;
  private String dbName;
  private String dbPass;
  private int dbPort;
  private String dbUser;
  private int port = 8080;
  private Map<String, ObjectNode> provisioners;
  private Map<String, RoutingParameterType> routerParameters;
  private List<Route> routing;
  private String url;
  private Map<String, ObjectNode> workflowEngines;

  public String getDbHost() {
    return dbHost;
  }

  public String getDbName() {
    return dbName;
  }

  public String getDbPass() {
    return dbPass;
  }

  public int getDbPort() {
    return dbPort;
  }

  public String getDbUser() {
    return dbUser;
  }

  public int getPort() {
    return port;
  }

  public Map<String, ObjectNode> getProvisioners() {
    return provisioners;
  }

  public Map<String, RoutingParameterType> getRouterParameters() {
    return routerParameters;
  }

  public List<Route> getRouting() {
    return routing;
  }

  public String getUrl() {
    return url;
  }

  public Map<String, ObjectNode> getWorkflowEngines() {
    return workflowEngines;
  }

  public void setDbHost(String dbHost) {
    this.dbHost = dbHost;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setDbPass(String dbPass) {
    this.dbPass = dbPass;
  }

  public void setDbPort(int dbPort) {
    this.dbPort = dbPort;
  }

  public void setDbUser(String dbUser) {
    this.dbUser = dbUser;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setProvisioners(Map<String, ObjectNode> provisioners) {
    this.provisioners = provisioners;
  }

  public void setRouterParameters(Map<String, RoutingParameterType> routerParameters) {
    this.routerParameters = routerParameters;
  }

  public void setRouting(List<Route> routing) {
    this.routing = routing;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setWorkflowEngines(Map<String, ObjectNode> workflowEngines) {
    this.workflowEngines = workflowEngines;
  }
}
